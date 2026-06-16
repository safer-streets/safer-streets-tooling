"""GeoDS Retail Centre Boundaries → ``retail_centres.parquet``."""

from safer_streets_core.database import duckdb_connector
from safer_streets_core.utils import data_dir

from safer_streets_tooling.config import data_source
from safer_streets_tooling.datasets._common import write_geoparquet
from safer_streets_tooling.datasets.base import Dataset, ExtractContext


def extract(ctx: ExtractContext) -> None:
    """
    Write the ``retail_centres`` parquet from the GeoDS Retail Centre Boundaries GeoPackage.

    Licensed (Geographic Data Service), so the GeoPackage is downloaded manually into the data
    directory. Unlike the other geometry sources this is supplied in WGS-84 (EPSG:4326), so the
    geometry is reprojected to BNG (EPSG:27700) on load to match the rest of the database (the H3
    nearest-centre lookup measures distances in metres).
    """
    gpkg_name = data_source("retail_centres")["gpkg"]
    gpkg = data_dir() / gpkg_name
    if not gpkg.exists():
        raise FileNotFoundError(
            f"GeoDS Retail Centre Boundaries GeoPackage not found: {gpkg}\n"
            f"Download {gpkg_name} from the Geographic Data Service (https://geods.ac.uk/) and place it in the data directory."
        )

    print(f"  Loading retail_centres from {gpkg}…")
    con = duckdb_connector(writeable=True)
    try:
        con.execute(f"""
            CREATE TABLE retail_centres AS
            SELECT
                rc.RC_ID AS rc_id,
                rc.RC_Name AS rc_name,
                rc.Classification AS classification,
                rc.Country AS country,
                rc.Region_NM AS region_nm,
                rc.H3_count AS h3_count,
                rc.Retail_N AS retail_n,
                rc.Area_km2 AS area_km2,
                ST_Transform(rc.geom, 'EPSG:4326', 'EPSG:27700', always_xy := true) AS geom
            FROM ST_Read('{gpkg}') rc;
        """)
        row_count = con.execute("SELECT COUNT(*) FROM retail_centres").fetchone()[0]  # ty:ignore[not-subscriptable]
        write_geoparquet(con, "SELECT * FROM retail_centres", ctx.parquet("retail_centres"))
    finally:
        con.close()
    print(f"  retail_centres: {row_count:,} rows")


DATASET = Dataset(name="retail_centres", table="retail_centres", extract=extract)
