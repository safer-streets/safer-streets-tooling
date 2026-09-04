"""``streetlight_counts_h3_9`` / ``streetlight_counts_hotspots`` — street lights counted per cell."""

import duckdb

from safer_streets_tooling.transform import hotspots
from safer_streets_tooling.transform.base import TransformStep, create_clause, table_exists

STREETLIGHTS_TABLE = "streetlights"
RESOLUTION = 9


def build(con: duckdb.DuckDBPyConnection, resolutions: list[int], replace: bool) -> None:
    """Create ``streetlight_counts_h3_9`` counting street lights per resolution-9 H3 cell.

    Keyed by ``spatial_id`` (the lowercase-hex res-9 cell, matching ``crime_counts_h3_9`` /
    ``h3_9_geogs``), so a consumer joins the count straight onto those by ``spatial_id``. The street
    lights extract already carries an ``h3_9_id``, so this is a plain group-and-count. No-op if the
    streetlights table is absent. ``resolutions`` is ignored — street lights are only carried at
    resolution 9 (their extract has a single ``h3_9_id``).
    """
    if not table_exists(con, STREETLIGHTS_TABLE):
        return
    con.execute(f"""
        {create_clause("TABLE", f"streetlight_counts_h3_{RESOLUTION}", replace=replace)} AS
        SELECT h3_9_id AS spatial_id, COUNT(*) AS streetlight_count
        FROM {STREETLIGHTS_TABLE}
        WHERE h3_9_id IS NOT NULL
        GROUP BY h3_9_id;
    """)


def outputs(con: duckdb.DuckDBPyConnection, resolutions: list[int]) -> list[str]:
    if not table_exists(con, STREETLIGHTS_TABLE):
        return []
    return [f"streetlight_counts_h3_{RESOLUTION}"]


def build_hotspots(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Create ``streetlight_counts_hotspots`` counting street lights per hotspot hex.

    The hexes don't line up with the H3 grid, so unlike the res-9 count this can't reuse the extract's
    ``h3_9_id`` and places each light by its BNG point instead. No-op if either input is absent.
    """
    if not (table_exists(con, STREETLIGHTS_TABLE) and hotspots.available(con)):
        return
    con.execute(f"""
        {create_clause("TABLE", "streetlight_counts_hotspots", replace=replace)} AS
        SELECT spatial_id, COUNT(*) AS streetlight_count
        FROM ({hotspots.placed_points(STREETLIGHTS_TABLE)})
        GROUP BY spatial_id;
    """)


def hotspot_outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    if not (table_exists(con, STREETLIGHTS_TABLE) and hotspots.available(con)):
        return []
    return ["streetlight_counts_hotspots"]


STEP = TransformStep(
    name="streetlight_counts",
    build=build,
    outputs=outputs,
    description="Street lights counted per resolution-9 H3 cell, keyed by spatial_id.",
    extract_inputs=(STREETLIGHTS_TABLE,),
)
