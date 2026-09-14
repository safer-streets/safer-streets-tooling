"""Shared helpers for dataset extractors.

Downloading, zip-member extraction, and geometry-column normalisation. The parquet read/write
primitives that move data between the in-memory *extract* phase and the *transform* phase live in
``safer_streets_core.database`` (``write_geoparquet`` / ``read_geoparquet``).

Geometry is British National Grid (EPSG:27700) everywhere by convention. Sources supplied in another
CRS are reprojected to BNG inside their extractor before being written.
"""

import shutil
from pathlib import Path
from zipfile import ZipFile

import duckdb
import requests
from safer_streets_core.utils import data_dir
from tqdm import tqdm

from safer_streets_tooling.beahiv_grid import cell_id_sql, register_encoder
from safer_streets_tooling.grids import BEAHIV_ID, H3_ID, H3_RESOLUTION


def cell_id_columns(con: duckdb.DuckDBPyConnection, lat: str, lon: str, bng_geom: str) -> str:
    """SQL for the grid-id columns a point layer carries: ``h3r9_id`` and ``beahiv202_id``.

    Every point extract tags each feature with the cell it falls in on both grids, so the transform can
    group by a column instead of doing a spatial join, and a consumer can join a feature straight to
    ``h3r9_geogs`` or ``beahiv202_geogs``. Emitting them from one place keeps the two layers' columns
    identically named and derived — the H3 id from the WGS-84 lat/lon the h3 extension wants, the
    BEAHIV id by arithmetic on the BNG point (see :mod:`safer_streets_tooling.beahiv_grid`).

    ``bng_geom`` should be the *same* point the lat/lon describe — for a polygon layer, its centroid —
    or a feature lands in unrelated cells on the two grids. Registers the BEAHIV encoder on ``con`` as
    a side effect, so a caller needs nothing but this fragment.
    """
    register_encoder(con)
    return (
        f"lower(hex(h3_latlng_to_cell({lat}, {lon}, {H3_RESOLUTION}))) AS {H3_ID}, "
        f"{cell_id_sql(bng_geom)} AS {BEAHIV_ID}"
    )


def raw_dir() -> Path:
    """Directory under the data dir holding raw source files (downloaded or manually placed).

    Created on access so a fresh download always has somewhere to land.
    """
    d = data_dir() / "raw"
    d.mkdir(parents=True, exist_ok=True)
    return d


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
