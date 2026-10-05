"""``beahiv202_descriptions`` — a human-readable location for every BEAHIV cell.

Resolves the ids in ``beahiv202_geogs`` and its lookups to names (OS Open Roads, OS Open Greenspace, GIAS
schools, ONS area names) and assembles two labels per cell:

* ``short_location`` — street-level: the two main roads and the cell's LAD, e.g.
  ``Old Steine / East Street, Brighton and Hove``. A cell with no named road falls back to its LSOA name
  (``Wiltshire 005B``).
* ``description`` — a sentence: character, main road, greenspace, school and MSOA, e.g.
  ``Suburban, on Epsom Road (A24), by Ashtead Park. Mole Valley 001.`` Clauses whose part is missing are
  dropped.

No retail centre: the nearest-centre lookup it read from the geogs is deprecated and not built by default
(see :mod:`.retail_centre_lookups`).

The components are kept as columns, so a consumer can reassemble or filter them; ``roads`` lists the four
longest named roads.

Every name source is optional, as it is for the lookups: a missing table or column contributes an empty
relation, and its clause drops out of the labels.

:func:`build_unit` reads only ``{unit.key}``-named relations, so it serves any grid with a ``*_geogs``:
:mod:`.hotspot_descriptions` builds ``hotspots_descriptions`` with it (without the school, which is placed
by a cell-id column only the BEAHIV and H3 grids carry).
"""

import duckdb

from safer_streets_tooling.transform import beahiv
from safer_streets_tooling.transform.base import (
    Grid,
    SpatialUnit,
    TransformStep,
    column_exists,
    create_clause,
    relation,
    table_exists,
)
from safer_streets_tooling.transform.ons_hierarchy import GEOGRAPHY_MAPPINGS

DATASET = "descriptions"

GREENSPACE_MIN_SHARE = 0.2  # a named greenspace must cover this share of the cell to be mentioned
GREENSPACE_IN_SHARE = 0.5  # ...and this share to say "in" rather than "by" (or, unbuilt, "Open space")

# OS Open Roads' road classes, weighted so a short stretch of A road outranks a longer cul-de-sac when
# picking the cell's main road. Unlisted classes (access roads, restricted local access) weigh 0.5.
ROAD_WEIGHTS = {"Motorway": 4, "A Road": 3, "B Road": 2, "Minor Road": 1, "Local Road": 1}


def _empty(geogs: str, columns: str) -> str:
    """A relation with ``geogs``' ``spatial_id`` type and the given typed NULL columns, but no rows.

    Stands in for a missing name source. ``spatial_id`` is taken from the geogs rather than typed by hand
    because it is a BIGINT on BEAHIV and a hex string on H3 and the hotspot hexes.
    """
    return f"SELECT spatial_id, {columns} FROM {geogs} WHERE false"


def _roads(con: duckdb.DuckDBPyConnection, unit: SpatialUnit, geogs: str) -> str:
    lookup = f"{unit.key}_road_network_lookup"
    if not (table_exists(con, lookup) and table_exists(con, "open_roads")):
        return _empty(geogs, "NULL::VARCHAR AS name_1, NULL::VARCHAR AS number, NULL::DOUBLE AS len, 0.0 AS score")
    weights = " ".join(f"WHEN '{k}' THEN {v}" for k, v in ROAD_WEIGHTS.items())
    return f"""
        SELECT l.spatial_id, r.name_1, r.road_classification_number AS number, sum(l.overlap_length) AS len,
               sum(l.overlap_length * CASE l.type {weights} ELSE 0.5 END) AS score
        FROM {lookup} l JOIN open_roads r ON l.road_id = r.id
        WHERE r.name_1 IS NOT NULL OR r.road_classification_number IS NOT NULL
        GROUP BY ALL
    """


def _greenspace(con: duckdb.DuckDBPyConnection, unit: SpatialUnit, geogs: str) -> str:
    lookup = f"{unit.key}_greenspace_lookup"
    if not (table_exists(con, lookup) and column_exists(con, "open_greenspace", "distName1")):
        return _empty(geogs, "NULL::VARCHAR AS greenspace, NULL::DOUBLE AS area")
    # sites sharing a name (a park split into several polygons) are summed
    return f"""
        SELECT l.spatial_id, g.distName1 AS greenspace, sum(l.overlap_area) AS area
        FROM {lookup} l JOIN open_greenspace g ON l.greenspace_id = g.id
        WHERE g.distName1 IS NOT NULL
        GROUP BY ALL
    """


def _schools(con: duckdb.DuckDBPyConnection, unit: SpatialUnit, geogs: str) -> str:
    # the school *site's* cell, tagged at extract; the geogs school_ids are walking-isochrone catchments,
    # which cover many cells around each school and so say little about where a cell is
    cell_col = f"{unit.key}_id"
    if not column_exists(con, "schools", cell_col):
        return _empty(geogs, "NULL::VARCHAR AS school")
    return f"""
        SELECT {cell_col} AS spatial_id, arg_max(establishmentname, (coalesce(schoolcapacity, 0), establishmentname)) AS school
        FROM schools GROUP BY 1
    """


def _prop(con: duckdb.DuckDBPyConnection, geogs: str, column: str) -> str:
    # a NULL overlap is a structural zero (no land cover of that kind); an absent column means the land
    # cover wasn't loaded, which is unknown rather than zero
    if not column_exists(con, geogs, column):
        return "NULL::DOUBLE"
    return f"coalesce(h.{column}, 0) / h.cell_area"


def _names(key: str) -> str:
    return f"LEFT JOIN {GEOGRAPHY_MAPPINGS[key]} {key} ON h.{key} = {key}.spatial_id"


def build_unit(con: duckdb.DuckDBPyConnection, unit: SpatialUnit, replace: bool) -> None:
    """Create ``{unit.key}_descriptions``: one row per cell of ``{unit.key}_geogs``."""
    geogs = f"{unit.key}_geogs"
    con.execute(f"""
        {create_clause("TABLE", relation(unit.key, DATASET), replace=replace)} AS
        WITH
        roads AS ({_roads(con, unit, geogs)}),
        main_road AS (
            SELECT spatial_id,
                   arg_max(CASE WHEN name_1 IS NOT NULL AND number IS NOT NULL THEN name_1 || ' (' || number || ')'
                                ELSE coalesce(name_1, number) END, (score, name_1, number)) AS road
            FROM roads GROUP BY 1
        ),
        -- by name alone, so the numbered links of a named road merge with it: "Briggate / Boar Lane"
        road_names AS (
            SELECT spatial_id, coalesce(name_1, number) AS road, sum(score) AS score, sum(len) AS len
            FROM roads GROUP BY ALL
        ),
        road_lists AS (
            SELECT spatial_id,
                   array_to_string(arg_max(road, (score, road), 2), ' / ') AS road_pair,
                   array_to_string(arg_max(road, (len, road), 4), ' / ') AS roads
            FROM road_names GROUP BY 1
        ),
        green AS ({_greenspace(con, unit, geogs)}),
        main_green AS (
            SELECT spatial_id, arg_max(greenspace, (area, greenspace)) AS greenspace, max(area) AS greenspace_area
            FROM green GROUP BY 1
        ),
        school AS ({_schools(con, unit, geogs)}),
        parts AS (
            SELECT h.spatial_id, lad24cd.lad24nm, msoa21cd.msoa21nm, lsoa21cd.lsoa21nm,
                   {_prop(con, geogs, "urban_overlap_area")} AS prop_urban,
                   {_prop(con, geogs, "suburban_overlap_area")} AS prop_suburban,
                   coalesce(mg.greenspace_area / h.cell_area, 0) AS greenspace_share,
                   CASE WHEN prop_urban IS NULL THEN NULL
                        WHEN prop_urban >= 0.5 THEN 'Urban'
                        WHEN prop_urban + prop_suburban >= 0.5 THEN 'Suburban'
                        WHEN prop_urban + prop_suburban >= 0.15 THEN 'Edge of built-up area'
                        -- a park inside a town is not "rural", though it has no built-up land cover
                        WHEN greenspace_share >= {GREENSPACE_IN_SHARE} THEN 'Open space'
                        ELSE 'Rural' END AS character,
                   mr.road, rl.road_pair, rl.roads, mg.greenspace, s.school,
                   -- ONS writes some LADs "Bristol, City of", which reads oddly at the end of a location
                   regexp_replace(lad24cd.lad24nm, '^(.*), City of$', 'City of \\1') AS lad_name
            FROM {geogs} h
            {_names("lad24cd")}
            {_names("msoa21cd")}
            {_names("lsoa21cd")}
            LEFT JOIN main_road mr ON h.spatial_id = mr.spatial_id
            LEFT JOIN road_lists rl ON h.spatial_id = rl.spatial_id
            LEFT JOIN main_green mg ON h.spatial_id = mg.spatial_id
            LEFT JOIN school s ON h.spatial_id = s.spatial_id
        ),
        labelled AS (
            SELECT *,
                concat_ws(', ', character, 'on ' || road,
                    CASE WHEN greenspace_share >= {GREENSPACE_IN_SHARE} THEN 'in ' || greenspace
                         WHEN greenspace_share >= {GREENSPACE_MIN_SHARE} THEN 'by ' || greenspace END,
                    'near ' || school) AS head
            FROM parts
        )
        SELECT spatial_id, lad24nm, msoa21nm, lsoa21nm, prop_urban, prop_suburban, greenspace_share, character,
               road, road_pair, roads, greenspace, school,
               concat_ws('. ', nullif(head, ''), coalesce(msoa21nm, lad24nm)) || '.' AS description,
               CASE WHEN road_pair IS NULL THEN coalesce(lsoa21nm, lad24nm)
                    ELSE concat_ws(', ', road_pair, lad_name) END AS short_location
        FROM labelled
        ORDER BY spatial_id
    """)


def build(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Build ``beahiv202_descriptions``."""
    if not beahiv.available(con):
        return
    beahiv.register_udfs(con)  # the lookups are views carrying the cell-polygon UDF call
    build_unit(con, beahiv.BEAHIV_UNIT, replace)


def outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [relation(beahiv.BEAHIV_UNIT.key, DATASET)] if beahiv.available(con) else []


STEP = TransformStep(
    name="beahiv_descriptions",
    build=build,
    outputs=outputs,
    grid=Grid.BEAHIV,
    description="One row per BEAHIV cell: a human-readable short_location and description, plus the named roads, greenspace and school they are built from.",
    depends_on=("beahiv_lookups", "beahiv_geogs"),
    extract_inputs=(
        "open_roads",
        "open_greenspace",
        "schools",
        *(GEOGRAPHY_MAPPINGS[k] for k in ("lad24cd", "msoa21cd", "lsoa21cd")),
    ),
)
