"""Overture Maps places (points of interest) → ``poi.parquet``."""

from overturemaps import core as overture
from safer_streets_core.database import duckdb_connector, write_geoparquet

from safer_streets_tooling.config import data_source
from safer_streets_tooling.extract.base import Dataset, ExtractContext

# Overture Maps places (POI), streamed from S3 via the overturemaps reader (no API key). The bounding
# box (England & Wales, WGS-84 xmin/ymin/xmax/ymax) and the kept categories live in
# config/data_sources.json under the "poi" key.
_POI = data_source("poi")
POI_BBOX = tuple(_POI["bbox"])
POI_CATEGORIES = tuple(_POI["categories"])


def extract(ctx: ExtractContext) -> None:
    """
    Write the ``poi`` parquet from Overture Maps places (points of interest).

    The Overture ``place`` theme is streamed from S3 (anonymous, no API key) straight into DuckDB via
    the overturemaps reader — no intermediate file. Only the categories of interest are kept, and
    geometry is transformed from WGS-84 to BNG (yielding a ``geom`` column). A resolution-9 H3 cell id
    (``h3_9_id``, lowercase hex) is derived from the native WGS-84 point for joining to the H3 grid.
    """
    print("  Streaming Overture places…")
    reader = overture.record_batch_reader("place", bbox=POI_BBOX)
    if reader is None:
        raise RuntimeError("Overture Maps download failed (record_batch_reader returned None)")

    con = duckdb_connector(writeable=True)
    try:
        # `reader` is consumed directly by DuckDB via an Arrow replacement scan
        con.execute(
            """
            CREATE TABLE poi AS SELECT
                id AS poi_id,
                ST_Transform(geometry, 'EPSG:4326', 'EPSG:27700', always_xy := true) AS geom,
                lower(hex(h3_latlng_to_cell(ST_Y(geometry), ST_X(geometry), 9))) AS h3_9_id,
                names.primary AS name,
                addresses[1].postcode AS postcode,
                basic_category,
                categories.primary AS primary_category,
                categories.alternate AS alternate_category
            FROM reader
            WHERE basic_category = ANY(?)
            """,
            [list(POI_CATEGORIES)],
        )
        row_count = con.execute("SELECT COUNT(*) FROM poi").fetchone()[0]  # ty:ignore[not-subscriptable]
        write_geoparquet(con, "SELECT * FROM poi", ctx.parquet("poi"))
    finally:
        con.close()
    print(f"  poi: {row_count:,} rows")


DATASET = Dataset(name="poi", table="poi", extract=extract)
