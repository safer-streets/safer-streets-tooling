"""Home Office hotspot hexes → ``hotspots.parquet``.

The hotspot analysis is published as a 350m hex grid: one polygon per hex flagged as a hotspot for at
least one offence class, carrying the police force area it belongs to and a ``hits`` string whose
letters say which offence classes flagged it. Supplied directly rather than downloaded, so the
GeoParquet is placed by hand under the data directory (path in ``config/data_sources.json``).

The grid is the second spatial unit the transform aggregates onto (alongside the H3 cells): the hex id
becomes ``spatial_id``, matching the key every ``*_hotspots`` / ``hotspots_*`` relation joins on.
"""

from safer_streets_core.database import duckdb_connector, write_geoparquet
from safer_streets_core.utils import data_dir

from safer_streets_tooling.config import data_source
from safer_streets_tooling.extract.base import Dataset, ExtractContext


def extract(ctx: ExtractContext) -> None:
    """
    Write the ``hotspots`` parquet from the supplied hotspot-hex GeoParquet.

    The source is already BNG (EPSG:27700) so no reprojection is needed; the hex id (``hex_index``) is
    renamed to ``spatial_id`` — the key the hotspot transforms and their consumers join on — and the
    ``pfa`` / ``hits`` attributes are carried through unchanged.
    """
    path = data_dir() / data_source("hotspots")["path"]
    if not path.exists():
        raise FileNotFoundError(
            f"Home Office hotspot hexes not found: {path}\nPlace the supplied hotspot GeoParquet there."
        )

    print(f"  Loading hotspots from {path}…")
    con = duckdb_connector(writeable=True)
    try:
        con.execute(f"""
            CREATE TABLE hotspots AS
            SELECT hex_index AS spatial_id, pfa, hits, geometry AS geom
            FROM read_parquet('{path.as_posix()}');
        """)
        row_count = con.execute("SELECT COUNT(*) FROM hotspots").fetchone()[0]  # ty:ignore[not-subscriptable]
        write_geoparquet(con, "SELECT * FROM hotspots", ctx.parquet("hotspots"))
    finally:
        con.close()
    print(f"  hotspots: {row_count:,} rows")


DATASET = Dataset(
    name="hotspots",
    table="hotspots",
    extract=extract,
    description="Home Office hotspot hexes (350m grid), keyed by spatial_id, with their force area and offence-class hits.",
)
