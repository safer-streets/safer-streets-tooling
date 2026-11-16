"""``osm_building_categories`` — one use ``category`` per OpenStreetMap building — and
``bh350eef_osm_building_counts``, those buildings counted per BEAHIV cell and category.

The ``osm_buildings`` extract keeps a building's use spread over eight tag columns (``building``, ``shop``,
``office``, ``amenity``, ``railway``, ``public_transport``, ``landuse``, ``leisure``) and its position relative to the
ground over four more (``layer``, ``level``, ``tunnel``, ``location``). This step condenses them: the use tags into a
single ``category``, the level tags into an ``underground`` flag. ``osm_building_categories`` has one row per building
(``osm_id``, ``category``, the raw ``building`` value, ``name``, ``area_m2``, ``underground`` and the centroid's
``h3r9_id`` / ``bh350eef_id``), with no geometry: join to ``osm_buildings`` on ``osm_id`` for the footprint.

Every building gets a category. What a building is used for (a pub, a shop, an office) outranks what it was built as
(``building=house``), and a building with neither is ``unspecified`` (a bare ``building=yes``, over a third of them)
or ``other`` (a ``building`` value in no category).

``bh350eef_osm_building_counts`` mirrors ``bh350eef_building_counts`` (the Verisk buildings): ``spatial_id``,
``category``, ``building_count`` and ``footprint_area`` (m²), each building in the cell of its centroid. Like that
table it covers every cell holding a building, not only the crime cells, so the step waits on nothing. OSM draws many
terraces and blocks as one polygon and has no buildings at all in places, so compare it with the Verisk counts by
area rather than by count, and not at all where OSM's coverage is thin.
"""

import duckdb

from safer_streets_tooling.beahiv_grid import KEY
from safer_streets_tooling.grids import BEAHIV_ID, H3_ID, relation
from safer_streets_tooling.osm_tags import Tags, category_case
from safer_streets_tooling.transform import beahiv
from safer_streets_tooling.transform.base import Grid, TransformStep, create_clause
from safer_streets_tooling.transform.beahiv_osm_landuse import CATEGORIES as LANDUSE_CATEGORIES

SOURCE = "osm_buildings"
TABLE = "osm_building_categories"
COUNTS_TABLE = relation(KEY, "osm_building_counts")

# A building's category: the first whose tags it has (None: any value but osm_tags.NOT_A_VALUE). Uses come before
# building types, so a pub in a building=house is hospitality and a shop under building=apartments is a shop. Where
# osm_landuse has the same category its tags are reused, so a pub is a pub on either table; the building=* values are
# added here. Stations are only those tagged as one: finding the rest by their platforms needs a join to osm_landuse.
CATEGORIES: dict[str, Tags] = {
    "hospitality": {**LANDUSE_CATEGORIES["hospitality"], "building": ["pub", "restaurant"]},
    "station": {**LANDUSE_CATEGORIES["station"], "building": ["train_station"]},
    "shop": {**LANDUSE_CATEGORIES["shop"], "building": ["retail", "supermarket", "shop", "kiosk"]},
    "office": {**LANDUSE_CATEGORIES["office"], "building": ["office"]},
    "education": {
        **LANDUSE_CATEGORIES["education"],
        "building": ["school", "university", "college", "kindergarten"],
    },
    "health": {**LANDUSE_CATEGORIES["health"], "building": ["hospital"]},
    "worship": {
        "amenity": ["place_of_worship"],
        "building": ["church", "chapel", "cathedral", "mosque", "temple", "synagogue"],
    },
    "civic": {
        "amenity": ["community_centre", "library", "townhall", "social_facility", "police", "fire_station"],
        "building": ["public", "civic", "government", "fire_station", "community_centre", "library"],
    },
    "leisure": {
        "leisure": None,
        "amenity": ["theatre", "cinema"],
        "building": ["sports_centre", "sports_hall", "stadium", "grandstand", "pavilion"],
    },
    "parking": {**LANDUSE_CATEGORIES["parking"], "building": ["parking"]},
    "hotel": {"building": ["hotel"]},
    "commercial": {"building": ["commercial"], "landuse": ["commercial", "retail"]},
    "industrial": {
        "building": ["industrial", "warehouse", "manufacture", "factory", "hangar", "storage_tank"],
        "landuse": ["industrial"],
    },
    "agricultural": {
        "building": ["farm_auxiliary", "barn", "farm", "greenhouse", "agricultural", "stable", "cowshed", "sty"],
        "landuse": ["farmyard"],
    },
    "residential": {
        "building": [
            "house",
            "residential",
            "semidetached_house",
            "detached",
            "apartments",
            "terrace",
            "bungalow",
            "static_caravan",
            "dormitory",
            "flats",
            "maisonette",
            "chalet",
            "cabin",
        ],
    },
    "outbuilding": {
        "building": ["garage", "garages", "shed", "roof", "service", "hut", "outbuilding", "conservatory", "carport"],
    },
}

UNSPECIFIED = "unspecified"
OTHER = "other"

# quoted, as in the extract: level and location are SQL keywords
CATEGORY = (
    f"coalesce({category_case(CATEGORIES, lambda key: f'"{key}"')}, "
    f"CASE WHEN building = 'yes' THEN '{UNSPECIFIED}' ELSE '{OTHER}' END)"
)

# Below ground by any of its level tags: an underground car park or ticket hall has no footprint at street level.
# layer and level are free text ("-1", "-1;0", "B1"); only a plain negative number counts.
UNDERGROUND = (
    'coalesce(TRY_CAST(layer AS DOUBLE) < 0, false) OR coalesce(TRY_CAST("level" AS DOUBLE) < 0, false) '
    "OR coalesce(tunnel, 'no') <> 'no' OR coalesce(\"location\" = 'underground', false)"
)


def build(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Create both tables. No-op until ``osm_buildings`` is extracted with its cell ids (:func:`.beahiv.tagged`).

    The counts leave out underground buildings and those with no cell (a centroid off the grid).
    """
    if not beahiv.tagged(con, SOURCE):
        return
    con.execute(f"""
        {create_clause("TABLE", TABLE, replace=replace)} AS
        SELECT osm_id, {CATEGORY} AS category, building, name, area_m2, {UNDERGROUND} AS underground,
               {H3_ID}, {BEAHIV_ID}
        FROM {SOURCE}
    """)
    con.execute(f"""
        {create_clause("TABLE", COUNTS_TABLE, replace=replace)} AS
        SELECT {BEAHIV_ID} AS spatial_id, category, COUNT(*) AS building_count, SUM(area_m2) AS footprint_area
        FROM {TABLE}
        WHERE {BEAHIV_ID} IS NOT NULL AND NOT underground
        GROUP BY {BEAHIV_ID}, category
    """)
    buildings, unspecified = con.execute(
        f"SELECT COUNT(*), COUNT(*) FILTER (WHERE category = '{UNSPECIFIED}') FROM {TABLE}"
    ).fetchone()  # ty:ignore[not-iterable]
    (cells,) = con.execute(f"SELECT COUNT(DISTINCT spatial_id) FROM {COUNTS_TABLE}").fetchone()  # ty:ignore[not-iterable]
    print(f"  {TABLE}: {buildings:,} buildings ({unspecified:,} {UNSPECIFIED}) in {cells:,} cells")


def outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [TABLE, COUNTS_TABLE] if beahiv.tagged(con, SOURCE) else []


STEP = TransformStep(
    name="beahiv_osm_buildings",
    build=build,
    outputs=outputs,
    grid=Grid.BEAHIV,
    description=(
        "OpenStreetMap buildings with their tags condensed to one use category each (osm_building_categories: "
        "osm_id, category, building, name, area_m2, underground, h3r9_id, bh350eef_id), and their count and "
        "footprint area per BEAHIV cell and category (bh350eef_osm_building_counts), placed by centroid."
    ),
    extract_inputs=(SOURCE,),
)
