"""OpenStreetMap points of interest → ``poi.parquet``.

Read from the same QuackOSM parse of Geofabrik's England and Wales PBFs as ``osm_landuse`` and ``osm_buildings``
(:func:`~safer_streets_tooling.extract.osm_landuse.landuse_geoparquet`), so it costs no second parse. A POI is an
OSM node whose ``amenity``, ``shop``, ``office`` or ``leisure`` tag puts it in one of :data:`CATEGORIES`. Only nodes:
a venue mapped as its building or site (a pub drawn as a building, a car park as its area) is a polygon, and is in
``osm_buildings`` or ``osm_landuse`` with the same tags.

This replaced Overture Maps places, which lists many more venues that aren't there: against the FSA food hygiene
register, OSM finds as many of the registered pubs, bars, restaurants and takeaways, and far fewer of its own are
missing from the register (see JOURNAL.md).
"""

from safer_streets_core.database import duckdb_connector, write_geoparquet

from safer_streets_tooling.extract import osm_landuse
from safer_streets_tooling.extract._common import cell_id_columns
from safer_streets_tooling.extract.base import Dataset, ExtractContext

# A feature's category: the first whose tags it has (None: any value but NOT_A_VALUE). The order settles features
# with several, e.g. a cafe tagged shop=bakery is food, a bookmaker gambling. Every key is one QuackOSM selects on
# (osm_landuse.TAGS_FILTER); adding a key there changes its cache key, so the next extract is a cold parse.
CATEGORIES: dict[str, dict[str, list[str] | None]] = {
    "adult": {"amenity": ["stripclub", "brothel", "swingerclub"]},
    "gambling": {
        "amenity": ["casino", "gambling"],
        "shop": ["bookmaker", "lottery"],
        "leisure": ["adult_gaming_centre"],
    },
    "alcohol": {"amenity": ["pub", "bar", "nightclub", "biergarten"]},
    "restaurant": {"amenity": ["restaurant"]},
    "food": {"amenity": ["cafe", "fast_food", "food_court", "ice_cream"]},
    "market": {"amenity": ["marketplace"]},
    "shop": {"shop": None, "amenity": ["pharmacy"]},
    "office": {"office": None},
    "hospital": {"amenity": ["hospital"]},
    "police": {"amenity": ["police"]},
    "atm": {"amenity": ["atm"]},
    "parking": {"amenity": ["parking"]},  # a car park mapped as a point; amenity=parking_space (one bay) isn't
}
# shop=vacant (about 10k), shop=no, ...: not a shop
NOT_A_VALUE = ["no", "vacant", "disused"]

# Tags kept as columns (raw values, NULL if absent), so a consumer can refine a category: shop=supermarket, ...
TAG_COLUMNS = {
    "amenity": "amenity",
    "shop": "shop",
    "office": "office",
    "leisure": "leisure",
    "name": "name",
    "brand": "brand",
    "addr:postcode": "postcode",
    "opening_hours": "opening_hours",
}


def _sql_list(values: list[str]) -> str:
    return ", ".join(f"'{v}'" for v in values)


def _has(key: str, values: list[str] | None) -> str:
    if values is None:
        return f"coalesce(tags['{key}'], 'no') NOT IN ({_sql_list(NOT_A_VALUE)})"
    return f"coalesce(tags['{key}'] IN ({_sql_list(values)}), false)"


CATEGORY = (
    "CASE "
    + " ".join(
        f"WHEN {' OR '.join(_has(key, values) for key, values in tags.items())} THEN '{category}'"
        for category, tags in CATEGORIES.items()
    )
    + " END"
)


def extract(ctx: ExtractContext) -> None:
    """Write ``poi``: one row per OSM POI node with its category, tags and grid cells."""
    src = osm_landuse.landuse_geoparquet(ctx)
    tag_cols = ", ".join(f"tags['{key}'] AS \"{col}\"" for key, col in TAG_COLUMNS.items())
    con = duckdb_connector(writeable=True)
    try:
        query = f"""
            SELECT osm_id, category, {tag_cols}, geom,
                   {cell_id_columns(con, "ST_X(geometry)", "ST_Y(geometry)", "geom")}
            FROM (
                SELECT feature_id AS osm_id, {CATEGORY} AS category, tags, geometry,
                       ST_Transform(geometry, 'EPSG:4326', 'EPSG:27700', always_xy := true) AS geom
                FROM read_parquet('{src}')
                WHERE starts_with(feature_id, 'node/')
            )
            WHERE category IS NOT NULL
        """
        write_geoparquet(con, query, ctx.parquet("poi"))
        row_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{ctx.parquet('poi')}')").fetchone()[0]  # ty:ignore[not-subscriptable]
    finally:
        con.close()
    print(f"  poi: {row_count:,} rows")


DATASET = Dataset(
    name="poi",
    table="poi",
    extract=extract,
    description=(
        "OpenStreetMap points of interest, nodes only (venues mapped as polygons are in osm_buildings / osm_landuse): "
        "one row per node with its category (alcohol, food, restaurant, shop, office, gambling, adult, market, "
        "hospital, police, atm, parking), amenity/shop/office/leisure tags, name, brand, postcode, opening_hours "
        "and grid cells."
    ),
    # not a parquet it reads: run after osm_landuse so it reuses QuackOSM's cached parse instead of a concurrent one
    depends_on=("osm_landuse",),
)
