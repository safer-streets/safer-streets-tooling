"""OpenStreetMap land use → ``osm_landuse.parquet``, and OpenStreetMap buildings → ``osm_buildings.parquet``.

Read from Geofabrik's England and Wales PBF extracts with `QuackOSM <https://github.com/kraina-ai/quackosm>`_, which
assembles polygons from closed ways and multipolygon relations (DuckDB's own ``ST_ReadOSM`` only yields raw nodes,
ways and relations). The selection is land use and land cover, pedestrian areas, buildings, shops, offices, and train
stations and their platforms. Roads are left out: OS Open Roads (``open_roads``) is the road network.

One parse is split by geometry role, one row per OSM way/relation (QuackOSM's ``way/123`` / ``relation/456``) with
its BNG polygon: ``osm_buildings`` holds every polygon with a ``building`` tag (other than ``building=no``), about 80%
of the rows but under 2% of the area, and ``osm_landuse`` everything else. Land-use polygons nest and overlap, so a
per-cell share needs their union; building footprints barely overlap, and the shops, offices and amenities drawn on
them can be counted, so the two tables are aggregated differently.
"""

import re
from pathlib import Path

import quackosm
import requests
from safer_streets_core.database import duckdb_connector, write_geoparquet

from safer_streets_tooling.config import data_source
from safer_streets_tooling.extract._common import download, raw_dir
from safer_streets_tooling.extract.base import Dataset, ExtractContext

# natural=* values that describe land cover; the others (bay, peninsula, ridge, tree_row, ...) name features
NATURAL_LAND_COVER = [
    "wood",
    "scrub",
    "heath",
    "grassland",
    "wetland",
    "water",
    "beach",
    "sand",
    "shingle",
    "bare_rock",
    "scree",
    "mud",
]
PEDESTRIAN_HIGHWAYS = ["pedestrian", "footway"]
# Train stations, and the platforms that identify the many station buildings tagged only building=yes (a building
# touched by several platforms) or building=roof (a train shed over them). That rule belongs to the transform; the
# extract keeps its inputs.
STATION_VALUES = ["station", "platform"]

# A feature is selected if it has any of these tags (True: any value). QuackOSM-style filter. No road network: OS
# Open Roads (open_roads) is the road layer, so the only highway=* features are pedestrian areas.
TAGS_FILTER: dict[str, bool | list[str]] = {
    "landuse": True,
    "leisure": True,
    "amenity": True,
    "natural": NATURAL_LAND_COVER,
    "building": True,
    "shop": True,
    "office": True,
    "railway": STATION_VALUES,
    "public_transport": STATION_VALUES,
    # pedestrian areas: squares, plazas and pedestrianised streets mapped as areas
    "highway": PEDESTRIAN_HIGHWAYS,
    "area:highway": PEDESTRIAN_HIGHWAYS,
    "place": ["square"],
}

# The keys that make a polygon land use or a building by themselves. The pedestrian keys don't: they only select a
# feature that is mapped as an area.
_PEDESTRIAN_KEYS = ("highway", "area:highway", "place")
LAND_KEYS = tuple(key for key in TAGS_FILTER if key not in _PEDESTRIAN_KEYS)

# Applied by QuackOSM to each element's tags before it builds any geometry. A highway=pedestrian|footway way is a
# line unless it is tagged as an area (or is a multipolygon); skipping the lines here saves assembling millions of
# footways only to drop them. The gate only applies to those two highway values, and not when another land-use key is
# present: a pedestrianised street tagged amenity=marketplace, or a park drawn on a footway loop, is a polygon without
# area=yes.
_OTHER_LANDUSE_KEYS = " OR ".join(f"map_contains(tags, '{key}')" for key in LAND_KEYS)
SQL_FILTER = (
    f"coalesce(tags['highway'], '') NOT IN ({', '.join(repr(v) for v in PEDESTRIAN_HIGHWAYS)})"
    " OR coalesce(tags['area'], '') = 'yes' OR coalesce(tags['type'], '') = 'multipolygon'"
    f" OR {_OTHER_LANDUSE_KEYS}"
)

# Applied to QuackOSM's output, after the parse. amenity=parking_space polygons are single bays (about 200k of them,
# median 12 m²) inside the amenity=parking car park that already covers them, so they're dropped, unless another
# land-use key makes the polygon land use in its own right. Filtering here rather than in SQL_FILTER keeps the bay
# rule out of QuackOSM's cache key.
_LAND_KEYS_BUT_AMENITY = " OR ".join(f"map_contains(tags, '{key}')" for key in LAND_KEYS if key != "amenity")
POLYGON_FILTER = f"(coalesce(tags['amenity'], '') <> 'parking_space' OR {_LAND_KEYS_BUT_AMENITY})"

# A building, as opposed to land use. building=no means "not a building" (a shop or amenity drawn as an area), so
# such polygons are land use.
BUILDING = "coalesce(tags['building'], 'no') <> 'no'"

# Each table's columns: the feature's raw value of each tag (NULL if absent), keyed by tag. All of a feature's tags
# among these are kept, not just the one that selected it: a polygon tagged leisure=park + landuse=grass is both, and
# which category it counts towards is the transform's decision. The level tags select nothing but tell an above-ground
# platform from an underground one (layer < 0, level < 0, tunnel, location=underground), so tube platforms needn't
# count towards a train station; on buildings they mark the ones hidden under another.
LEVEL_TAGS = ("layer", "level", "tunnel", "location")
LANDUSE_TAGS = (*(key for key in TAGS_FILTER if key != "building"), *LEVEL_TAGS)
# A building's use (shop, office, amenity=pub, railway=station, ...) and the land-use tags a few carry
BUILDING_TAGS = (
    "building",
    "shop",
    "office",
    "amenity",
    "railway",
    "public_transport",
    "landuse",
    "leisure",
    *LEVEL_TAGS,
)


def _column(key: str) -> str:
    """``area:highway`` → ``area_highway``."""
    return key.replace(":", "_")


_PBF_NAME = re.compile(r"(?P<region>[a-z-]+)-\d{6}\.osm\.pbf")


def osm_pbf(region: str, *, force_download: bool = False) -> Path:
    """The region's newest cached Geofabrik PBF, downloading the current snapshot if there is none (or if forced).

    ``-latest`` redirects to a dated file (``wales-261108.osm.pbf``); the dated name is the one cached, so the snapshot
    in use is visible in the raw folder. A forced download resolves the redirect and only fetches if that snapshot
    isn't already on disk — same name, same bytes.
    """
    cached = sorted(raw_dir().glob(f"{region}-[0-9]*.osm.pbf"))  # YYMMDD sorts chronologically
    if cached and not force_download:
        return cached[-1]
    response = requests.head(data_source("osm")["url"].format(region=region), allow_redirects=True, timeout=60)
    response.raise_for_status()
    name = response.url.rsplit("/", 1)[-1]
    match = _PBF_NAME.fullmatch(name)
    if not match or match["region"] != region:
        raise ValueError(f"expected {region}-YYMMDD.osm.pbf from Geofabrik's -latest redirect, got {response.url}")
    dest = raw_dir() / name
    if dest.exists():
        print(f"  Using cached {dest} (current snapshot)")
    else:
        download(response.url, dest)
    return dest


def landuse_geoparquet(ctx: ExtractContext) -> Path:
    """QuackOSM's GeoParquet of every selected feature in England & Wales (any geometry, WGS-84, all tags as a MAP).

    QuackOSM caches the result in ``raw/quackosm`` under a name hashing the PBFs and the filters, so the second of
    the two datasets (and any rerun on the same snapshot) reads it instead of re-parsing (several minutes for
    England). QuackOSM merges the two files, keeping one copy of a feature that crosses the border.
    """
    pbfs = [osm_pbf(region, force_download=ctx.force_download) for region in data_source("osm")["regions"]]
    print(f"  Parsing {', '.join(p.name for p in pbfs)} with QuackOSM…")
    return Path(
        quackosm.convert_pbf_to_parquet(
            pbf_path=pbfs,
            tags_filter=TAGS_FILTER,
            custom_sql_filter=SQL_FILTER,
            keep_all_tags=True,  # for name, which isn't a filter key
            explode_tags=False,
            working_directory=raw_dir() / "quackosm",
            verbosity_mode="silent",
        )
    )


def _polygons_sql(src: Path, tags: tuple[str, ...], where: str) -> str:
    """The polygonal features of QuackOSM's output that pass POLYGON_FILTER and ``where``, as ``osm_id``, a column
    per tag in ``tags``, ``name``, ``area_m2`` and the BNG ``geom`` (points and lines matched a key but aren't land
    use or buildings)."""
    # quoted: natural, level and location are SQL keywords
    tag_cols = "".join(f"tags['{key}'] AS \"{_column(key)}\", " for key in tags)
    return f"""
        SELECT osm_id, {tag_cols}tags['name'] AS name, ST_Area(geom) AS area_m2, geom
        FROM (
            SELECT feature_id AS osm_id, tags,
                   ST_Transform(geometry, 'EPSG:4326', 'EPSG:27700', always_xy := true) AS geom
            FROM read_parquet('{src}')
            WHERE ST_GeometryType(geometry) IN ('POLYGON', 'MULTIPOLYGON') AND {POLYGON_FILTER} AND {where}
        )
    """


def _write(ctx: ExtractContext, name: str, tags: tuple[str, ...], where: str) -> None:
    src = landuse_geoparquet(ctx)
    con = duckdb_connector(writeable=True)
    try:
        write_geoparquet(con, _polygons_sql(src, tags, where), ctx.parquet(name))
        row_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{ctx.parquet(name)}')").fetchone()[0]  # ty:ignore[not-subscriptable]
    finally:
        con.close()
    print(f"  {name}: {row_count:,} rows")


def extract_landuse(ctx: ExtractContext) -> None:
    """Write ``osm_landuse``: one row per land-use polygon with its tag values, name, area (m²) and geometry."""
    _write(ctx, "osm_landuse", LANDUSE_TAGS, f"NOT ({BUILDING})")


def extract_buildings(ctx: ExtractContext) -> None:
    """Write ``osm_buildings``: one row per building polygon with its tag values, name, area (m²) and geometry."""
    _write(ctx, "osm_buildings", BUILDING_TAGS, BUILDING)


DATASETS = (
    Dataset(
        name="osm_landuse",
        table="osm_landuse",
        extract=extract_landuse,
        description=(
            "OpenStreetMap land-use polygons (landuse/leisure/amenity/land-cover natural, shop and office areas, "
            "railway and public_transport station/platform tags, and pedestrian areas; no buildings, roads or "
            "parking_space bays), one row per OSM way/relation (osm_id) with its tag values, the platform level "
            "tags, name, area_m2 and BNG geom."
        ),
    ),
    Dataset(
        name="osm_buildings",
        table="osm_buildings",
        extract=extract_buildings,
        description=(
            "OpenStreetMap building polygons (building=* other than no), one row per OSM way/relation (osm_id) with "
            "its building, shop, office, amenity, station and land-use tags, level tags, name, area_m2 and BNG geom."
        ),
        # not a parquet it reads: run after osm_landuse so it reuses QuackOSM's cached parse instead of a second,
        # concurrent one
        depends_on=("osm_landuse",),
    ),
)
