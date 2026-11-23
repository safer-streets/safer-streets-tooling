"""``bh350eef_osm_landuse_lookup`` — the area of every BEAHIV cell each OpenStreetMap land-use polygon covers.

One row per (cell, polygon): the cell's ``spatial_id``, the polygon's ``osm_id``, ``overlap_area`` (the area, m², of
the polygon inside the cell) and the polygon's land-use ``category``. Polygons in no category are left out (private gardens, verges, nature reserves, ...: the ``osm_landuse`` extract still has them).

An area rather than a percentage, as for every overlap in this package (``overlap_area`` in the overlap lookups,
``{prefix}_overlap_area`` in ``*_geogs``): a percentage leaves open whether it is of the cell or of the polygon. Divide
by ``cell_area`` (or :data:`~safer_streets_tooling.beahiv_grid.CELL_AREA`) for the share of the cell.

Land-use polygons nest (a car park on retail land, a pitch in a park), so a cell's ``overlap_area`` can sum past its
area, and summing it per category double counts the nesting. An exact per-category share needs the union of the category's
polygons in each cell, which this table can't give without the geometry.

The cells are the ones carrying crimes (:data:`~.beahiv.BEAHIV_UNIT`), as for every lookup on this grid, so the step
waits on ``beahiv_counts``. It is a step of its own rather than an
:class:`~.overlap_lookups.OverlapFeature`: those are built on every grid and folded into ``*_geogs`` as one value per
cell, and this is a long table on the BEAHIV grid only.
"""

import duckdb

from safer_streets_tooling.beahiv_grid import KEY
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


def build_lookup(con: duckdb.DuckDBPyConnection, table: str, features: str, replace: bool) -> None:
    """Create ``table``: every feature of ``features`` intersected with every crime cell it overlaps.

    ``features`` is a query giving ``osm_id``, ``category`` and ``geom``. Shared with ``beahiv_osm_buildings``, so the
    two lookups have the same columns and the same measure of overlap.

    A table rather than a view (the overlap lookups are views): the intersections are the costly part, and a view
    would recompute them for every read, the parquet write included. Rows for a polygon that only touches a cell's
    edge (zero overlap) are dropped.
    """
    beahiv.register_udfs(con)
    con.execute(f"""
        {create_clause("TABLE", table, replace=replace)} AS
        WITH clipped AS (
            SELECT c.spatial_id, f.osm_id,
                   ST_Area(ST_Intersection(c.cell_geom, f.geom)) AS overlap_area,
                   f.category
            FROM ({beahiv.BEAHIV_UNIT.cells}) c
            JOIN ({features}) f ON ST_Intersects(c.cell_geom, f.geom)
        )
        SELECT * FROM clipped WHERE overlap_area > 0
    """)
    rows, cells = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT spatial_id) FROM {table}").fetchone()  # ty:ignore[not-iterable]
    print(f"  {table}: {rows:,} overlaps in {cells:,} cells")


def build(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Create the lookup from every categorised land-use polygon (see :func:`build_lookup`)."""
    if not _available(con):
        return
    features = f"SELECT * FROM (SELECT osm_id, {CATEGORY} AS category, geom FROM {SOURCE}) WHERE category IS NOT NULL"
    build_lookup(con, TABLE, features, replace)


def outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [TABLE] if _available(con) else []


STEP = TransformStep(
    name="beahiv_osm_landuse",
    build=build,
    outputs=outputs,
    grid=Grid.BEAHIV,
    description=(
        "Per BEAHIV cell, every overlapping OpenStreetMap land-use polygon (osm_id) with its land-use category and "
        "overlap_area, the area of the cell it covers in m² (nested polygons can sum past the cell's area)."
    ),
    depends_on=("beahiv_counts",),
    extract_inputs=(SOURCE,),
)
