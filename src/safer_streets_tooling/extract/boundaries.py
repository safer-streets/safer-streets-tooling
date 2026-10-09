"""ONS boundary layers → one parquet per layer (``police_force_areas.parquet`` … ``output_areas_2021.parquet``).

Each ONS Open Geography Portal layer in the ``boundaries`` catalogue becomes its own dataset, named
after its final table. The id field is renamed to ``spatial_id`` (matching the H3 geography lookups in
``safer_streets_core.transforms``). Features are paged from the portal's ArcGIS REST API in BNG
(``outSR=27700``, so no client-side reprojection) and cached as a GeoPackage under the raw data directory,
reused unless force_download.
"""

import io
import json
import time
from functools import cache
from pathlib import Path
from typing import Any

import geopandas as gpd
import requests
from safer_streets_core.database import duckdb_connector, write_geoparquet
from safer_streets_core.utils import data_source

from safer_streets_tooling.extract._common import raw_dir
from safer_streets_tooling.extract.base import Dataset, ExtractContext

PAGE_SIZE = 2000
BNG_SRID = "27700"


@cache
def sources() -> dict[str, Any]:
    """ONS boundary catalogue (``base_url`` + per-layer endpoint / filename / table / id_field)."""
    return data_source("boundaries")


def _query(session: requests.Session, endpoint: str, params: dict[str, Any], timeout: int) -> dict[str, Any]:
    resp = session.get(endpoint, params=params, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    # ArcGIS reports query errors in a 200 response body, not the status code
    if "error" in data:
        raise RuntimeError(f"ArcGIS error: {data['error']}")
    return data


def _fetch_page(session: requests.Session, endpoint: str, offset: int, page_size: int, retries: int = 3) -> list[dict]:
    params = {
        "where": "1=1",
        "outFields": "*",
        "outSR": BNG_SRID,
        "f": "geojson",
        "resultOffset": offset,
        "resultRecordCount": page_size,
    }
    for attempt in range(1, retries + 1):
        try:
            return _query(session, endpoint, params, timeout=120).get("features", [])
        except (requests.RequestException, RuntimeError) as exc:
            if attempt == retries:
                raise
            wait = 2**attempt
            print(f"    Attempt {attempt} failed ({exc}), retrying in {wait}s…")
            time.sleep(wait)
    return []


def fetch_all_features(layer_key: str, session: requests.Session) -> list[dict]:
    """Page through the layer's ArcGIS feature service and return every feature as a GeoJSON dict (BNG)."""
    endpoint = f"{sources()['base_url']}{sources()['layers'][layer_key]['endpoint']}"
    total = _query(session, endpoint, {"where": "1=1", "returnCountOnly": "true", "f": "json"}, timeout=60)["count"]
    print(f"  {total:,} features found")

    features: list[dict] = []
    while len(features) < total:
        page = _fetch_page(session, endpoint, len(features), min(PAGE_SIZE, total - len(features)))
        if not page:
            print(f"  Warning: empty page at offset {len(features)}, stopping early.")
            break
        features.extend(page)
    print(f"  Downloaded {len(features):,} features.")
    return features


def write_geopackage(features: list[dict], gpkg_path: Path) -> None:
    """Write BNG GeoJSON features to a GeoPackage, lower-casing column names (``ST_Read`` can't)."""
    gdf = gpd.read_file(io.StringIO(json.dumps({"type": "FeatureCollection", "features": features})))
    gdf = gdf.rename(columns={col: col.lower().replace(" ", "_") for col in gdf.columns})
    # GeoJSON carries no CRS, so the reader assumes WGS-84; the coordinates are BNG because we asked for outSR
    gdf = gdf.set_crs("EPSG:27700", allow_override=True)
    # a GPKG write would otherwise add a second layer to an existing file
    gpkg_path.unlink(missing_ok=True)
    gdf.to_file(gpkg_path, driver="GPKG")


def _make_extract(layer_key: str, table: str):
    def extract(ctx: ExtractContext) -> None:
        info = sources()["layers"][layer_key]
        gpkg = raw_dir() / f"{info['filename']}_bng.gpkg"
        if ctx.force_download or not gpkg.exists():
            session = requests.Session()
            session.headers.update({"User-Agent": "ONS-Boundary-Downloader/1.0"})
            write_geopackage(fetch_all_features(layer_key, session), gpkg)
        else:
            print(f"  Using cached {gpkg}")

        con = duckdb_connector(writeable=True)
        try:
            # ST_Read returns a geometry column named 'geom'
            con.execute(f"CREATE TABLE \"{table}\" AS SELECT * FROM ST_Read('{gpkg}');")
            con.execute(f'ALTER TABLE "{table}" RENAME COLUMN {info["id_field"]} TO spatial_id;')
            row_count = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]  # ty:ignore[not-subscriptable]
            write_geoparquet(con, f'SELECT * FROM "{table}"', ctx.parquet(table))
        finally:
            con.close()
        print(f"  {table}: {row_count:,} rows")

    return extract


# one-line description per boundary table, surfaced in the index.parquet catalogue. Any table without an
# entry falls back to a generic phrasing.
_DESCRIPTIONS = {
    "police_force_areas": "ONS Police Force Area boundaries (E&W), id renamed to spatial_id (pfa24cd).",
    "local_authority_districts": "ONS Local Authority District boundaries (UK), spatial_id = lad24cd.",
    "msoa_2021": "ONS 2021 MSOA boundaries (E&W), spatial_id = msoa21cd.",
    "lsoa_2021": "ONS 2021 LSOA boundaries (E&W), spatial_id = lsoa21cd.",
    "output_areas_2021": "ONS 2021 Output Area boundaries (E&W), spatial_id = oa21cd.",
}


def _datasets() -> tuple[Dataset, ...]:
    return tuple(
        Dataset(
            name=info["table"],
            table=info["table"],
            extract=_make_extract(layer_key, info["table"]),
            description=_DESCRIPTIONS.get(
                info["table"], f"ONS {info['table']} boundary layer (spatial_id code + geom)."
            ),
            optional=False,  # the H3 geography lookups in transform/geo_lookups.py require every boundary table
        )
        for layer_key, info in sources()["layers"].items()
    )


DATASETS: tuple[Dataset, ...] = _datasets()
