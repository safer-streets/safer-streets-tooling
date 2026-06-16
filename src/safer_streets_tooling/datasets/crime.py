"""police.uk street-level crime archive → ``crime_data.parquet``."""

from safer_streets_core.database import duckdb_connector
from safer_streets_core.utils import archive_path

from safer_streets_tooling.config import data_source
from safer_streets_tooling.datasets._common import download, write_geoparquet
from safer_streets_tooling.datasets.base import Dataset, ExtractContext


def extract(ctx: ExtractContext) -> None:
    """
    Write the ``crime_data`` parquet from the police.uk bulk crime archive.

    The latest archive is downloaded (cached under the data directory, reused unless force_download)
    and every month/force ``*-street.csv`` is read via DuckDB's zipfs. Geometry is added by
    transforming the WGS-84 longitude/latitude to BNG (EPSG:27700); the lon/lat columns are retained
    because the H3 transforms index crimes straight from them.
    """
    archive = archive_path("latest")
    if ctx.force_download or not archive.exists():
        download(data_source("crime")["url"], archive)
    else:
        print(f"  Using cached {archive}")

    con = duckdb_connector(writeable=True)
    try:
        con.execute("INSTALL zipfs FROM community;LOAD zipfs;")
        # limited support for **/ glob, but ????-?? is a reasonable workaround
        con.execute(f"""
            CREATE TABLE crime_data AS
            SELECT * FROM read_csv('zip://{archive}/????-??/*-street.csv', normalize_names = true);
            ALTER TABLE crime_data ADD COLUMN geom GEOMETRY;
            UPDATE crime_data
            SET geom = ST_Transform(
                    ST_Point(longitude, latitude),
                    'EPSG:4326',
                    'EPSG:27700',
                    always_xy := true
                );
        """)
        row_count = con.execute("SELECT COUNT(*) FROM crime_data").fetchone()[0]  # ty:ignore[not-subscriptable]
        write_geoparquet(con, "SELECT * FROM crime_data", ctx.parquet("crime_data"))
    finally:
        con.close()
    print(f"  crime_data: {row_count:,} rows")


DATASET = Dataset(name="crime_data", table="crime_data", extract=extract, optional=False)
