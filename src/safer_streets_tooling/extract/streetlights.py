"""Street lights from Overture Maps base/infrastructure → ``streetlights.parquet``.

Overture's ``infrastructure`` type carries OSM street furniture; street lights are the
``subtype = 'transportation'`` / ``class = 'street_lamp'`` points (OSM ``highway=street_lamp``). They
are streamed from S3 (no API key), filtered, and their WGS-84 geometry is reprojected to BNG, with a
resolution-9 ``h3_9_id`` (lowercase hex) for joining to the H3 grid. Street lights carry no useful
attributes beyond their id and location, so only those are kept.
"""

from overturemaps import core as overture
from safer_streets_core.database import duckdb_connector, write_geoparquet

from safer_streets_tooling.config import data_source
from safer_streets_tooling.extract.base import Dataset, ExtractContext

# Overture base/infrastructure street lights (OSM highway=street_lamp), streamed from S3 via the
# overturemaps reader (no API key). The bounding box (England & Wales, WGS-84 xmin/ymin/xmax/ymax) and
# the subtype/class filter live in config/data_sources.json under the "streetlights" key.
_STREETLIGHTS = data_source("streetlights")
STREETLIGHTS_BBOX = tuple(_STREETLIGHTS["bbox"])
STREETLIGHTS_SUBTYPE = _STREETLIGHTS["subtype"]
STREETLIGHTS_CLASS = _STREETLIGHTS["class"]


def extract(ctx: ExtractContext) -> None:
    """
    Write the ``streetlights`` parquet from Overture Maps base/infrastructure.

    The Overture ``infrastructure`` theme is streamed from S3 (anonymous, no API key) straight into
    DuckDB via the overturemaps reader — no intermediate file. Only street lights (the configured
    subtype/class) are kept, geometry is transformed from WGS-84 to BNG (yielding a ``geom`` column),
    and a resolution-9 H3 cell id (``h3_9_id``, lowercase hex) is derived from the native WGS-84 point.
    """
    print("  Streaming Overture infrastructure…")
    reader = overture.record_batch_reader("infrastructure", bbox=STREETLIGHTS_BBOX)
    if reader is None:
        raise RuntimeError("Overture Maps download failed (record_batch_reader returned None)")

    con = duckdb_connector(writeable=True)
    try:
        # `reader` is consumed directly by DuckDB via an Arrow replacement scan
        con.execute(
            """
            CREATE TABLE streetlights AS SELECT
                id AS streetlight_id,
                ST_Transform(geometry, 'EPSG:4326', 'EPSG:27700', always_xy := true) AS geom,
                lower(hex(h3_latlng_to_cell(ST_Y(geometry), ST_X(geometry), 9))) AS h3_9_id
            FROM reader
            WHERE subtype = ? AND class = ?
            """,
            [STREETLIGHTS_SUBTYPE, STREETLIGHTS_CLASS],
        )
        row_count = con.execute("SELECT COUNT(*) FROM streetlights").fetchone()[0]  # ty:ignore[not-subscriptable]
        write_geoparquet(con, "SELECT * FROM streetlights", ctx.parquet("streetlights"))
    finally:
        con.close()
    print(f"  streetlights: {row_count:,} rows")


DATASET = Dataset(name="streetlights", table="streetlights", extract=extract)
