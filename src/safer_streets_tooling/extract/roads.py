"""OS Open Roads network → ``open_roads.parquet``."""

from zipfile import ZipFile

from safer_streets_core.database import duckdb_connector, write_geoparquet

from safer_streets_tooling.config import data_source
from safer_streets_tooling.extract._common import download, extract_cached, raw_dir, rename_geom_column
from safer_streets_tooling.extract.base import Dataset, ExtractContext


def extract(ctx: ExtractContext) -> None:
    """
    Write the ``open_roads`` parquet from the OS Open Roads dataset.

    The GB geopackage (open data, no API key) is downloaded from the OS Downloads API and cached
    under the data directory (reused unless force_download). OS Open Roads names its geometry column
    'geometry'; it is normalised to 'geom' for consistency with the other tables.
    """
    src = data_source("roads")
    zip_path = raw_dir() / src["zip"]
    if ctx.force_download or not zip_path.exists():
        download(src["url"], zip_path)
    else:
        print(f"  Using cached {zip_path}")

    with ZipFile(zip_path) as z:
        members = [name for name in z.namelist() if name.endswith(src["layer"])]
    if not members:
        raise FileNotFoundError(f"{src['layer']} not found in {zip_path}")

    gpkg = extract_cached(zip_path, members[0])
    con = duckdb_connector(writeable=True)
    try:
        con.execute(f"CREATE TABLE open_roads AS SELECT * FROM ST_Read('{gpkg}', layer='road_link');")
        rename_geom_column(con, "open_roads")
        row_count = con.execute("SELECT COUNT(*) FROM open_roads").fetchone()[0]  # ty:ignore[not-subscriptable]
        write_geoparquet(con, "SELECT * FROM open_roads", ctx.parquet("open_roads"))
    finally:
        con.close()
    print(f"  open_roads: {row_count:,} rows")


DATASET = Dataset(name="open_roads", table="open_roads", extract=extract)
