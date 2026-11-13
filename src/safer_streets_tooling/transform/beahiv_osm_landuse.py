"""``bh350eef_osm_landuse_lookup`` — each OpenStreetMap land-use polygon's share of every BEAHIV cell it overlaps.

One row per (cell, polygon): the cell's ``spatial_id``, the polygon's ``osm_id``, ``overlap_pct`` (the part of the
polygon inside the cell, as a percentage of the cell's area) and the polygon's land-use ``category``. Polygons in no
category are left out (private gardens, verges, nature reserves, ...: the ``osm_landuse`` extract still has them).

Land-use polygons nest (a car park on retail land, a pitch in a park), so a cell's ``overlap_pct`` can sum past 100,
and summing it per category double counts the nesting. An exact per-category share needs the union of the category's
polygons in each cell, which this table can't give without the geometry.

The cells are the ones carrying crimes (:data:`~.beahiv.BEAHIV_UNIT`), as for every lookup on this grid, so the step
waits on ``beahiv_counts``. It is a step of its own rather than an
:class:`~.overlap_lookups.OverlapFeature`: those are built on every grid and folded into ``*_geogs`` as one value per
cell, and this is a long table on the BEAHIV grid only.
"""

import duckdb

from safer_streets_tooling.beahiv_grid import CELL_AREA, KEY
from safer_streets_tooling.osm_tags import Tags, category_case
from safer_streets_tooling.transform import beahiv
from safer_streets_tooling.transform.base import Grid, TransformStep, create_clause, table_exists

SOURCE = "osm_landuse"
TABLE = f"{KEY}_osm_landuse_lookup"

PEDESTRIAN_HIGHWAYS = ["pedestrian", "footway"]

# A polygon's category: the first whose tags it has (None: any value but osm_tags.NOT_A_VALUE). Specific uses come
# before the zones they sit in, so a car park on retail land is parking and a school's grounds are education rather
# than residential. Drafted in safer-streets-eda's osm-landuse.ipynb, less what needs osm_buildings: building-type
# shops and offices (building=retail, ...), and train stations found by their platforms or roof.
CATEGORIES: dict[str, Tags] = {
    "hospitality": {"amenity": ["pub", "bar", "nightclub", "restaurant", "fast_food", "cafe", "biergarten"]},
    "station": {"railway": ["station"], "public_transport": ["station"]},
    "parking": {"amenity": ["parking", "bicycle_parking", "motorcycle_parking"]},
    "shop": {"shop": None},
    "office": {"office": None},
    "education": {"amenity": ["school", "college", "university", "kindergarten"], "landuse": ["education"]},
    "health": {"amenity": ["hospital", "clinic", "doctors"], "landuse": ["healthcare"]},
    "pedestrian": {"highway": PEDESTRIAN_HIGHWAYS, "area:highway": PEDESTRIAN_HIGHWAYS, "place": ["square"]},
    "sport": {"leisure": ["pitch", "sports_centre", "stadium", "track", "golf_course", "sports_hall"]},
    "park": {
        "leisure": ["park", "common", "playground", "dog_park"],
        "landuse": ["recreation_ground", "village_green"],
    },
    "cemetery": {"landuse": ["cemetery", "religious"], "amenity": ["grave_yard"]},
    "transport": {"landuse": ["railway", "highway"], "amenity": ["fuel", "bus_station", "ferry_terminal"]},
    "retail": {"landuse": ["retail"]},
    "commercial": {"landuse": ["commercial"]},
    "industrial": {"landuse": ["industrial", "port", "depot", "garages"]},
    "residential": {"landuse": ["residential"]},
    "vacant": {"landuse": ["brownfield", "construction", "greenfield"]},
    "woodland": {"landuse": ["forest", "forestry"], "natural": ["wood", "scrub"]},
    "water": {"natural": ["water", "wetland"], "landuse": ["reservoir", "basin"]},
    "moorland": {"natural": ["heath", "grassland", "bare_rock", "scree", "fell", "moor"]},
    "agriculture": {"landuse": ["farmland", "meadow", "farmyard", "orchard", "animal_keeping", "vineyard"]},
}

# the extract's column for a tag (area:highway -> area_highway), quoted because natural is an SQL keyword
CATEGORY = category_case(CATEGORIES, lambda key: '"{}"'.format(key.replace(":", "_")))


def _available(con: duckdb.DuckDBPyConnection) -> bool:
    return beahiv.available(con) and table_exists(con, SOURCE)


def build(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Create the lookup: every categorised polygon intersected with every crime cell it overlaps.

    A table rather than a view (the overlap lookups are views): the intersections are the costly part, and a view
    would recompute them for every read, the parquet write included. Rows for a polygon that only touches a cell's
    edge (zero overlap) are dropped.
    """
    if not _available(con):
        return
    beahiv.register_udfs(con)
    con.execute(f"""
        {create_clause("TABLE", TABLE, replace=replace)} AS
        WITH landuse AS (
            SELECT * FROM (SELECT osm_id, {CATEGORY} AS category, geom FROM {SOURCE}) WHERE category IS NOT NULL
        ),
        clipped AS (
            SELECT c.spatial_id, l.osm_id,
                   100 * ST_Area(ST_Intersection(c.cell_geom, l.geom)) / {CELL_AREA!r} AS overlap_pct,
                   l.category
            FROM ({beahiv.BEAHIV_UNIT.cells}) c
            JOIN landuse l ON ST_Intersects(c.cell_geom, l.geom)
        )
        SELECT * FROM clipped WHERE overlap_pct > 0
    """)
    rows, cells = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT spatial_id) FROM {TABLE}").fetchone()  # ty:ignore[not-iterable]
    print(f"  {TABLE}: {rows:,} overlaps in {cells:,} cells")


def outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [TABLE] if _available(con) else []


STEP = TransformStep(
    name="beahiv_osm_landuse",
    build=build,
    outputs=outputs,
    grid=Grid.BEAHIV,
    description=(
        "Per BEAHIV cell, every overlapping OpenStreetMap land-use polygon (osm_id) with its land-use category and "
        "overlap_pct, the percentage of the cell it covers (nested polygons can sum past 100)."
    ),
    depends_on=("beahiv_counts",),
    extract_inputs=(SOURCE,),
)
