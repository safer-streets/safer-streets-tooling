"""``bh350eef_osm_buildings_lookup`` — the area of every BEAHIV cell each OpenStreetMap building covers.

The buildings' counterpart of ``bh350eef_osm_landuse_lookup``, with the same columns: one row per (cell, building)
with the cell's ``spatial_id``, the building's ``osm_id``, ``overlap_area`` (the area, m², of the footprint inside the
cell) and the building's ``category``. A building straddling cells has a row in each.

The ``osm_buildings`` extract keeps a building's use spread over eight tag columns (``building``, ``shop``,
``office``, ``amenity``, ``railway``, ``public_transport``, ``landuse``, ``leisure``); ``category`` condenses them to
one. What a building is used for (a pub, a shop, an office) outranks what it was built as (``building=house``), and
unlike land use every building has a category: one with no use and no known type is ``unspecified`` (a bare
``building=yes``, over a third of them) or ``other`` (a ``building`` value in no category). The level tags
(``layer``, ``level``, ``tunnel``, ``location``) say where a building is rather than what it is for: a building they
put below ground has no footprint at street level and is left out.

Footprints barely overlap one another, so unlike land use a cell's ``overlap_area`` summed per category is close to
the area that category truly covers. The cells are the ones carrying crimes (:data:`~.beahiv.BEAHIV_UNIT`), so
the step waits on ``beahiv_counts``.
"""

import duckdb

from safer_streets_tooling.beahiv_grid import KEY
from safer_streets_tooling.osm_tags import Tags, category_case
from safer_streets_tooling.transform import beahiv
from safer_streets_tooling.transform.base import Grid, TransformStep, table_exists
from safer_streets_tooling.transform.beahiv_osm_landuse import CATEGORIES as LANDUSE_CATEGORIES
from safer_streets_tooling.transform.beahiv_osm_landuse import build_lookup

SOURCE = "osm_buildings"
TABLE = f"{KEY}_osm_buildings_lookup"

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


def _available(con: duckdb.DuckDBPyConnection) -> bool:
    return beahiv.available(con) and table_exists(con, SOURCE)


def build(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Create the lookup from every building above ground (see :func:`.beahiv_osm_landuse.build_lookup`)."""
    if not _available(con):
        return
    features = f"SELECT osm_id, {CATEGORY} AS category, geom FROM {SOURCE} WHERE NOT ({UNDERGROUND})"
    build_lookup(con, TABLE, features, replace)


def outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [TABLE] if _available(con) else []


STEP = TransformStep(
    name="beahiv_osm_buildings",
    build=build,
    outputs=outputs,
    grid=Grid.BEAHIV,
    description=(
        "Per BEAHIV cell, every overlapping OpenStreetMap building (osm_id) above ground with its use category "
        "(its tags condensed to one value) and overlap_area, the area of the cell its footprint covers in m²."
    ),
    depends_on=("beahiv_counts",),
    extract_inputs=(SOURCE,),
)
