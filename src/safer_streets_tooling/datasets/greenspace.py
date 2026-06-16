"""OS Open Greenspace polygons → ``open_greenspace.parquet``."""

from zipfile import ZipFile

from safer_streets_core.database import duckdb_connector, write_geoparquet

from safer_streets_tooling.config import data_source
from safer_streets_tooling.datasets._common import download, raw_dir
from safer_streets_tooling.datasets.base import Dataset, ExtractContext


def extract(ctx: ExtractContext) -> None:
    """
    Write the ``open_greenspace`` parquet from the OS Open Greenspace polygons.

    The GB shapefile bundle (open data, no API key) is downloaded from the OS Downloads API and
    cached under the data directory (reused unless force_download). The bundle is a zip of the
    GreenspaceSite (polygon) and AccessPoint (point) layers; we load the polygons. Data is already in
    BNG (EPSG:27700). The polygon layer is read straight from the zip via GDAL's /vsizip; ST_Read
    yields a ``geom`` column.
    """
    src = data_source("greenspace")
    zip_path = raw_dir() / src["zip"]
    if ctx.force_download or not zip_path.exists():
        download(src["url"], zip_path)
    else:
        print(f"  Using cached {zip_path}")

    with ZipFile(zip_path) as z:
        members = [name for name in z.namelist() if name.endswith(src["layer"])]
    if not members:
        raise FileNotFoundError(f"{src['layer']} not found in {zip_path}")

    # ENCODING=ISO-8859-1 matches the OS Open Greenspace shapefile
    vsizip = f"/vsizip/{zip_path}/{members[0]}"
    con = duckdb_connector(writeable=True)
    try:
        con.execute(f"""
            CREATE TABLE open_greenspace AS
            SELECT * FROM ST_Read('{vsizip}', open_options=['ENCODING=ISO-8859-1']);
        """)
        row_count = con.execute("SELECT COUNT(*) FROM open_greenspace").fetchone()[0]  # ty:ignore[not-subscriptable]
        write_geoparquet(con, "SELECT * FROM open_greenspace", ctx.parquet("open_greenspace"))
    finally:
        con.close()
    print(f"  open_greenspace: {row_count:,} rows")


DATASET = Dataset(name="open_greenspace", table="open_greenspace", extract=extract)
