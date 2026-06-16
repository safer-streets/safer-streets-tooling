"""Shared helpers for dataset extractors.

Downloading, zip-member extraction, geometry-column normalisation, and the parquet read/write
primitives that move data between the in-memory *extract* phase and the *assemble* phase.

Geometry is British National Grid (EPSG:27700) everywhere by convention. The DuckDB GEOMETRY type
carries no CRS, so its native GeoParquet writer tags written geometry as ``OGC:CRS84``; that label is
not relied upon — the coordinates are the contract, and ``database.index_geometry_tables`` strips the
CRS qualifier back to a bare ``GEOMETRY`` on assemble. Sources supplied in another CRS are reprojected
to BNG inside their extractor before being written.
"""

import shutil
from pathlib import Path
from zipfile import ZipFile

import duckdb
import requests
from tqdm import tqdm


def download(url: str, dest: Path) -> None:
    """Stream ``url`` to ``dest`` with a progress bar."""
    print(f"  Downloading {dest}…")
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()
    size = int(response.headers.get("content-length", 0))
    with open(dest, "wb") as fd, tqdm(total=size, unit="B", unit_scale=True) as bar:
        for chunk in response.iter_content(1024**2):
            bar.update(len(chunk))
            fd.write(chunk)


def extract_cached(zip_path: Path, member: str) -> Path:
    """
    Extract a single zip member to a cached file beside the zip and return its path.

    ST_Read over /vsizip is much slower for a GeoPackage than reading an extracted file:
    GPKG is SQLite, so every random seek forces /vsizip to re-decompress from a sync point.
    The extracted file is cached and only re-extracted if the zip is newer.
    """
    dest = zip_path.with_suffix("") / Path(member).name
    if not dest.exists() or dest.stat().st_mtime < zip_path.stat().st_mtime:
        dest.parent.mkdir(parents=True, exist_ok=True)
        print(f"  Extracting {member}…")
        with ZipFile(zip_path) as z, z.open(member) as src, open(dest, "wb") as out:
            shutil.copyfileobj(src, out)
    return dest


def rename_geom_column(con: duckdb.DuckDBPyConnection, table: str) -> None:
    """Rename ``table``'s geometry column to 'geom' if it has some other name (no-op if already 'geom')."""
    geom_cols = [
        row[0]
        for row in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = ? AND table_schema = 'main' AND data_type LIKE 'GEOMETRY%'",
            [table],
        ).fetchall()
    ]
    if geom_cols and geom_cols[0] != "geom":
        con.execute(f'ALTER TABLE "{table}" RENAME COLUMN "{geom_cols[0]}" TO geom;')


def write_geoparquet(con: duckdb.DuckDBPyConnection, query: str, out_path: Path) -> None:
    """Dump ``query`` (a ``geom`` GEOMETRY column is written as GeoParquet WKB) to ``out_path``.

    A temp file is written then moved into place so a crash never leaves a half-written parquet.
    """
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    con.execute(f"COPY ({query}) TO '{tmp}' (FORMAT parquet);")
    tmp.replace(out_path)


def read_geoparquet(path: Path) -> str:
    """SQL reading a dataset parquet back; a ``geom`` column returns as GEOMETRY directly (BNG assumed).

    Wrap in ``CREATE [OR REPLACE] TABLE <name> AS <this>`` to materialise it.
    """
    return f"SELECT * FROM read_parquet('{path}')"
