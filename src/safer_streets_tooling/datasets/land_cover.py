"""UKCEH Land Cover Map vector GeoPackage → ``land_cover.parquet``."""

from zipfile import ZipFile

from safer_streets_core.database import duckdb_connector, write_geoparquet

from safer_streets_tooling.config import data_source
from safer_streets_tooling.datasets._common import extract_cached, raw_dir
from safer_streets_tooling.datasets.base import Dataset, ExtractContext


def extract(ctx: ExtractContext) -> None:
    """
    Write the ``land_cover`` parquet from the UKCEH Land Cover Map vector GeoPackage.

    Licensed (EIDC, https://catalogue.ceh.ac.uk/) so it cannot be auto-downloaded — the LCM vector
    bundle zip is located under the data directory (the ``.gpkg`` inside it is read; already BNG /
    EPSG:27700). ST_Read yields a ``geom`` column (with ``gid`` and ``_mode``).

    We only care about built-up land, so the raw classes are filtered to urban (``_mode`` 20) and
    suburban (``_mode`` 21) and dissolved into merged polygons (per the land-cover notebook): the two
    classes are unioned, made valid, and dumped to individual polygons. Output columns are
    ``id``, ``urban`` (true for urban, false for suburban), and ``geom``.
    """
    zip_name = data_source("land_cover")["zip"]
    zip_path = raw_dir() / zip_name
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
        # Filter to built-up classes (urban=20, suburban=21) and dissolve into merged polygons.
        con.execute(f"""
            CREATE TABLE land_cover AS
            SELECT row_number() OVER () AS gid, urban, geom
            FROM (
                SELECT urban, UNNEST(ST_Dump(ST_Union_Agg(ST_MakeValid(geom)))).geom AS geom
                FROM (
                    SELECT _mode::INT = 20 AS urban, geom
                    FROM ST_Read('{gpkg}')
                    WHERE _mode::INT > 19
                )
                GROUP BY urban
            );
        """)
        row_count = con.execute("SELECT COUNT(*) FROM land_cover").fetchone()[0]  # ty:ignore[not-subscriptable]
        write_geoparquet(con, "SELECT * FROM land_cover", ctx.parquet("land_cover"))
    finally:
        con.close()
    print(f"  land_cover: {row_count:,} rows")


DATASET = Dataset(name="land_cover", table="land_cover", extract=extract)
