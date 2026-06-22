"""CCTV / surveillance cameras from OpenStreetMap (Overpass) → ``cctv.parquet``.

OSM ``man_made=surveillance`` nodes pulled from the Overpass API (no API key) for the England & Wales
bbox; mirrors the ``streetlights`` extract (``cctv_id`` / ``geom`` / ``h3_9_id``). The WGS-84
Longitude/Latitude become a point ``geom`` (reprojected to BNG) plus a resolution-9 ``h3_9_id``
(lowercase hex) for joining to the H3 grid. Like streetlights, OSM CCTV coverage is uneven, so this is
a presence/indicative signal rather than a complete inventory.
"""

import requests
from safer_streets_core.database import duckdb_connector, write_geoparquet

from safer_streets_tooling.config import data_source
from safer_streets_tooling.extract.base import Dataset, ExtractContext

# OSM CCTV (man_made=surveillance) via the Overpass API (no API key). The bounding box (England & Wales,
# WGS-84 xmin/ymin/xmax/ymax), the Overpass endpoint and the OSM tag live in config/data_sources.json
# under the "cctv" key.
_CCTV = data_source("cctv")
CCTV_BBOX = tuple(_CCTV["bbox"])
OVERPASS_URL = _CCTV["overpass_url"]
CCTV_TAG = _CCTV["tag"]

# Overpass rejects the default python-requests User-Agent with HTTP 406 (anti-abuse), so identify the
# tool explicitly (per Overpass etiquette).
_HEADERS = {"User-Agent": "safer-streets-tooling (+https://github.com/safer-streets/safer-streets-tooling)"}


def _overpass_query(bbox: tuple[float, ...], tag: str) -> str:
    """Build an Overpass QL query for ``tag`` nodes within ``bbox`` (WGS-84 xmin/ymin/xmax/ymax).

    Overpass expects the bbox as (south, west, north, east) = (ymin, xmin, ymax, xmax).
    """
    key, _, value = tag.partition("=")
    xmin, ymin, xmax, ymax = bbox
    return f'[out:json][timeout:300];node["{key}"="{value}"]({ymin},{xmin},{ymax},{xmax});out body;'


def extract(ctx: ExtractContext) -> None:
    """
    Write the ``cctv`` parquet from OSM ``man_made=surveillance`` nodes (Overpass API).

    Nodes are pulled from Overpass for the England & Wales bbox; their WGS-84 Longitude/Latitude become a
    point ``geom`` (reprojected to BNG, EPSG:27700) and a resolution-9 H3 cell id (``h3_9_id``, lowercase
    hex). Schema matches the ``streetlights`` extract: ``cctv_id`` / ``geom`` / ``h3_9_id``.
    """
    query = _overpass_query(CCTV_BBOX, CCTV_TAG)
    print(f"  Querying Overpass for CCTV ({CCTV_TAG})…")
    resp = requests.post(OVERPASS_URL, data={"data": query}, headers=_HEADERS, timeout=600)
    resp.raise_for_status()
    elements = resp.json()["elements"]
    # (id, lon, lat) — Overpass returns WGS-84 lon/lat on each node
    rows = [(f"node/{e['id']}", e["lon"], e["lat"]) for e in elements if "lon" in e and "lat" in e]
    if not rows:
        raise RuntimeError("Overpass returned no CCTV (man_made=surveillance) nodes")

    con = duckdb_connector(writeable=True)
    try:
        con.execute("CREATE TABLE _cctv (cctv_id VARCHAR, lon DOUBLE, lat DOUBLE)")
        con.executemany("INSERT INTO _cctv VALUES (?, ?, ?)", rows)
        con.execute(
            """
            CREATE TABLE cctv AS SELECT
                cctv_id,
                ST_Transform(ST_Point(lon, lat), 'EPSG:4326', 'EPSG:27700', always_xy := true) AS geom,
                lower(hex(h3_latlng_to_cell(lat, lon, 9))) AS h3_9_id
            FROM _cctv
            """
        )
        row_count = con.execute("SELECT COUNT(*) FROM cctv").fetchone()[0]  # ty:ignore[not-subscriptable]
        write_geoparquet(con, "SELECT * FROM cctv", ctx.parquet("cctv"))
    finally:
        con.close()
    print(f"  cctv: {row_count:,} rows")


DATASET = Dataset(name="cctv", table="cctv", extract=extract)
