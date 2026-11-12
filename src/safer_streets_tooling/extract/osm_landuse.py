"""OpenStreetMap land-use polygons → ``osm_landuse.parquet`` (attributes) + ``osm_landuse_geom.parquet`` (geometry).

Read from Geofabrik's England and Wales PBF extracts with `QuackOSM <https://github.com/kraina-ai/quackosm>`_, which
assembles polygons from closed ways and multipolygon relations (DuckDB's own ``ST_ReadOSM`` only yields raw nodes,
ways and relations). The selection matches the ``LANDUSE`` / ``PEDESTRIAN_AREA`` filters explored in
safer-streets-eda's ``osm-landuse.ipynb``.

One feature becomes one row in each of two tables, joined on ``osm_id`` (QuackOSM's ``way/123`` / ``relation/456``):
the attributes, without geometry, so they can be filtered and summarised cheaply, and the BNG polygons on their own.
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

# A feature is selected if it has any of these tags (True: any value). QuackOSM-style filter.
TAGS_FILTER: dict[str, bool | list[str]] = {
    "landuse": True,
    "leisure": True,
    "amenity": True,
    "natural": NATURAL_LAND_COVER,
    # pedestrian areas: squares, plazas and pedestrianised streets mapped as areas
    "highway": PEDESTRIAN_HIGHWAYS,
    "area:highway": PEDESTRIAN_HIGHWAYS,
    "place": ["square"],
}

# Applied by QuackOSM to each element's tags before it builds any geometry. Buildings are left out, so an
# amenity=pub building isn't counted as land use. A highway=pedestrian|footway way is a line unless it is tagged as
# an area (or is a multipolygon); skipping the lines here saves assembling millions of footways only to drop them.
# The gate only applies to those two highway values, and not when another land-use key is present: a pedestrianised
# street tagged amenity=marketplace, or a park drawn on a footway loop, is a polygon without area=yes.
_OTHER_LANDUSE_KEYS = " OR ".join(
    f"map_contains(tags, '{key}')" for key in ("landuse", "leisure", "amenity", "natural")
)
SQL_FILTER = (
    "NOT map_contains(tags, 'building') AND ("
    f"coalesce(tags['highway'], '') NOT IN ({', '.join(repr(v) for v in PEDESTRIAN_HIGHWAYS)})"
    " OR coalesce(tags['area'], '') = 'yes' OR coalesce(tags['type'], '') = 'multipolygon'"
    f" OR {_OTHER_LANDUSE_KEYS})"
)

# Each filter key becomes a column holding the feature's raw value (NULL if absent). All of them are kept, not just
# the one that matched first: a polygon tagged leisure=park + landuse=grass is both, and which category it counts
# towards is the transform's decision.
TAG_COLUMNS = {key: key.replace(":", "_") for key in TAGS_FILTER}

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


def _polygons_sql(src: Path) -> str:
    """The polygonal features of QuackOSM's output, in BNG (points and lines matched a key but aren't land use)."""
    return f"""
        SELECT feature_id AS osm_id, tags,
               ST_Transform(geometry, 'EPSG:4326', 'EPSG:27700', always_xy := true) AS geom
        FROM read_parquet('{src}')
        WHERE ST_GeometryType(geometry) IN ('POLYGON', 'MULTIPOLYGON')
    """


def extract_landuse(ctx: ExtractContext) -> None:
    """Write ``osm_landuse``: one row per land-use polygon with its tag values, name and area (m²), no geometry."""
    src = landuse_geoparquet(ctx)
    tag_cols = ", ".join(f"tags['{key}'] AS {col}" for key, col in TAG_COLUMNS.items())
    con = duckdb_connector(writeable=True)
    try:
        query = f"SELECT osm_id, {tag_cols}, tags['name'] AS name, ST_Area(geom) AS area_m2 FROM ({_polygons_sql(src)})"
        write_geoparquet(con, query, ctx.parquet("osm_landuse"))
        row_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{ctx.parquet('osm_landuse')}')").fetchone()[0]  # ty:ignore[not-subscriptable]
    finally:
        con.close()
    print(f"  osm_landuse: {row_count:,} rows")


def extract_landuse_geom(ctx: ExtractContext) -> None:
    """Write ``osm_landuse_geom``: the BNG polygon of each ``osm_landuse`` row, keyed on ``osm_id``."""
    src = landuse_geoparquet(ctx)
    con = duckdb_connector(writeable=True)
    try:
        write_geoparquet(con, f"SELECT osm_id, geom FROM ({_polygons_sql(src)})", ctx.parquet("osm_landuse_geom"))
        row_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{ctx.parquet('osm_landuse_geom')}')").fetchone()[0]  # ty:ignore[not-subscriptable]
    finally:
        con.close()
    print(f"  osm_landuse_geom: {row_count:,} rows")


DATASETS = (
    Dataset(
        name="osm_landuse",
        table="osm_landuse",
        extract=extract_landuse,
        description=(
            "OpenStreetMap land-use polygons (landuse/leisure/amenity/land-cover natural tags and pedestrian areas; "
            "no buildings), one row per OSM way/relation (osm_id) with its tag values, name and area_m2. "
            "Geometry is in osm_landuse_geom."
        ),
        geometry=False,
    ),
    Dataset(
        name="osm_landuse_geom",
        table="osm_landuse_geom",
        extract=extract_landuse_geom,
        description="BNG polygons of the osm_landuse features, keyed on osm_id.",
        # not a parquet it reads: run after osm_landuse so it reuses QuackOSM's cached parse instead of a second,
        # concurrent one
        depends_on=("osm_landuse",),
    ),
)
