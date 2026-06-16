"""UKCEH Land Cover Map vector GeoPackage → ``land_cover.parquet``."""

from zipfile import ZipFile

from safer_streets_core.database import duckdb_connector
from safer_streets_core.utils import data_dir

from safer_streets_tooling.config import data_source
from safer_streets_tooling.datasets._common import extract_cached, write_geoparquet
from safer_streets_tooling.datasets.base import Dataset, ExtractContext


def extract(ctx: ExtractContext) -> None:
    """
    Write the ``land_cover`` parquet from the UKCEH Land Cover Map vector GeoPackage.

    Licensed (EIDC, https://catalogue.ceh.ac.uk/) so it cannot be auto-downloaded — the LCM vector
    bundle zip is located under the data directory (the ``.gpkg`` inside it is read; already BNG /
    EPSG:27700). ST_Read yields a ``geom`` column (with ``gid`` and ``_mode``).
    """
    zip_name = data_source("land_cover")["zip"]
    zip_path = data_dir() / zip_name
    if not zip_path.exists():
        raise FileNotFoundError(
            f"UKCEH Land Cover Map GeoPackage not found: {zip_path}\n"
            f"Download the LCM vector bundle from the EIDC (https://catalogue.ceh.ac.uk/) and place the zip "
            f"(named {zip_name}) in the data directory."
        )

    with ZipFile(zip_path) as z:
        members = [name for name in z.namelist() if name.endswith(".gpkg")]
    if not members:
        raise FileNotFoundError(f"No .gpkg found in {zip_path}")

    gpkg = extract_cached(zip_path, members[0])
    print(f"  Loading land_cover from {gpkg}…")
    con = duckdb_connector(writeable=True)
    try:
        con.execute(f"CREATE TABLE land_cover AS SELECT * FROM ST_Read('{gpkg}');")
        row_count = con.execute("SELECT COUNT(*) FROM land_cover").fetchone()[0]  # ty:ignore[not-subscriptable]
        write_geoparquet(con, "SELECT * FROM land_cover", ctx.parquet("land_cover"))
    finally:
        con.close()
    print(f"  land_cover: {row_count:,} rows")


DATASET = Dataset(name="land_cover", table="land_cover", extract=extract)
