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


def cell_id_columns(con: duckdb.DuckDBPyConnection, lon: str, lat: str, bng_geom: str) -> str:
    """SQL for the grid-id columns a point layer carries: ``h3r9_id`` and ``bh350eef_id``.

    Every point extract tags each feature with the cell it falls in on both grids, so the transform can
    group by a column instead of doing a spatial join, and a consumer can join a feature straight to
    ``h3r9_geogs`` or ``bh350eef_geogs``. Emitting them from one place keeps the two layers' columns
    identically named and derived — the H3 id from the WGS-84 lon/lat the h3 extension wants, the
    BEAHIV id by arithmetic on the BNG point (see :mod:`safer_streets_tooling.beahiv_grid`).

    ``bng_geom`` should be the *same* point the lon/lat describe — for a polygon layer, its centroid —
    or a feature lands in unrelated cells on the two grids. Registers the BEAHIV encoder on ``con`` as
    a side effect, so a caller needs nothing but this fragment.
    """
    register_encoder(con)
    return (
        # the h3 extension's signature is latitude-first
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
    """Stream ``url`` to ``dest`` with a progress bar, resuming an interrupted download where the server allows.

    Bytes go to ``<dest>.part``, which is renamed to ``dest`` only once complete. Extractors treat an
    existing ``dest`` as a cached copy, so writing to it directly would let an interrupted download pass
    as a good one on every later run.

    To resume, the validator (``ETag``, else ``Last-Modified``) of the response that started the ``.part``
    is kept beside it in ``<dest>.part.validator``. A rerun asks for the remaining bytes with ``If-Range``
    set to it, so the server sends either the rest of the same file (206) or, if the file has changed
    since, the whole new one (200), which starts the download again. Without a validator there is
    nothing to check a resume against, so the download starts again. Raises ``OSError`` if the server
    announced a length and the stream ended short of it.
    """
    part = dest.with_name(dest.name + ".part")
    validator_file = dest.with_name(dest.name + ".part.validator")
    # identity: the bytes on disk are the bytes on the wire, so byte ranges and content-length line up
    headers = {"Accept-Encoding": "identity"}
    done = part.stat().st_size if part.exists() else 0
    if done and validator_file.exists():
        headers |= {"Range": f"bytes={done}-", "If-Range": validator_file.read_text()}
        print(f"  Resuming {dest} from {done:,} bytes…")
    else:
        done = 0
        print(f"  Downloading {dest}…")

    with requests.get(url, headers=headers, stream=True, timeout=60) as response:
        if response.status_code == 416:  # the .part is longer than the file: it's from another version
            part.unlink()
            validator_file.unlink(missing_ok=True)
            return download(url, dest)
        response.raise_for_status()
        if response.status_code != 206:
            done = 0
            validator = response.headers.get("ETag") or response.headers.get("Last-Modified")
            if validator:
                validator_file.write_text(validator)
            else:
                validator_file.unlink(missing_ok=True)
        remaining = int(response.headers.get("content-length", 0))
        total = done + remaining if remaining else None
        with (
            open(part, "ab" if done else "wb") as fd,
            tqdm(initial=done, total=total, unit="B", unit_scale=True) as bar,
        ):
            for chunk in response.iter_content(1024**2):
                bar.update(len(chunk))
                fd.write(chunk)

    if total is not None and (size := part.stat().st_size) != total:
        raise OSError(f"Incomplete download of {url}: {size:,} of {total:,} bytes in {part}. Run again to resume.")
    part.replace(dest)
    validator_file.unlink(missing_ok=True)


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
