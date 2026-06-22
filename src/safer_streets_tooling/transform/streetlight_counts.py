"""``streetlight_counts_h3_9`` — street lights counted per resolution-9 H3 cell."""

import duckdb

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


STEP = TransformStep(
    name="streetlight_counts",
    build=build,
    outputs=outputs,
    extract_inputs=(STREETLIGHTS_TABLE,),
)
