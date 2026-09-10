"""``h3r9_streetlight_counts`` / ``hotspots_streetlight_counts`` — street lights counted per cell."""

import duckdb

from safer_streets_tooling.transform import hotspots
from safer_streets_tooling.transform.base import Grid, TransformStep, create_clause, h3_key, relation, table_exists

STREETLIGHTS_TABLE = "streetlights"
DATASET = "streetlight_counts"
RESOLUTION = 9


def build(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Create ``h3r9_streetlight_counts`` counting street lights per resolution-9 H3 cell.

    Keyed by ``spatial_id`` (the lowercase-hex res-9 cell, matching ``h3r9_crime_counts`` /
    ``h3r9_geogs``), so a consumer joins the count straight onto those by ``spatial_id``. The street
    lights extract already carries an ``h3r9_id``, so this is a plain group-and-count. No-op if the
    streetlights table is absent. Only resolution 9 is ever produced — the extract carries a single
    ``h3r9_id``.
    """
    if not table_exists(con, STREETLIGHTS_TABLE):
        return
    con.execute(f"""
        {create_clause("TABLE", relation(h3_key(RESOLUTION), DATASET), replace=replace)} AS
        SELECT h3r9_id AS spatial_id, COUNT(*) AS streetlight_count
        FROM {STREETLIGHTS_TABLE}
        WHERE h3r9_id IS NOT NULL
        GROUP BY h3r9_id;
    """)


def outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    if not table_exists(con, STREETLIGHTS_TABLE):
        return []
    return [relation(h3_key(RESOLUTION), DATASET)]


def build_hotspots(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Create ``hotspots_streetlight_counts`` counting street lights per hotspot hex.

    The hexes don't line up with the H3 grid, so unlike the res-9 count this can't reuse the extract's
    ``h3r9_id`` and places each light by its BNG point instead. No-op if either input is absent.
    """
    if not (table_exists(con, STREETLIGHTS_TABLE) and hotspots.available(con)):
        return
    con.execute(f"""
        {create_clause("TABLE", relation(hotspots.HOTSPOT_UNIT.key, DATASET), replace=replace)} AS
        SELECT spatial_id, COUNT(*) AS streetlight_count
        FROM ({hotspots.placed_points(STREETLIGHTS_TABLE)})
        GROUP BY spatial_id;
    """)


def hotspot_outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    if not (table_exists(con, STREETLIGHTS_TABLE) and hotspots.available(con)):
        return []
    return [relation(hotspots.HOTSPOT_UNIT.key, DATASET)]


STEP = TransformStep(
    name="streetlight_counts",
    build=build,
    outputs=outputs,
    grid=Grid.H3,
    description="Street lights counted per resolution-9 H3 cell, keyed by spatial_id.",
    extract_inputs=(STREETLIGHTS_TABLE,),
)
