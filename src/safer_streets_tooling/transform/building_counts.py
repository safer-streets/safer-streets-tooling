"""``h3r9_building_counts`` / ``hotspots_building_counts`` — buildings counted per cell and ``map_simple_use``."""

import duckdb

from safer_streets_tooling.transform import hotspots
from safer_streets_tooling.transform.base import Grid, TransformStep, create_clause, h3_key, relation, table_exists

BUILDINGS_TABLE = "buildings"
DATASET = "building_counts"
RESOLUTION = 9


def build(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Create ``h3r9_building_counts`` counting buildings per resolution-9 H3 cell / ``map_simple_use``.

    Keyed by ``spatial_id`` (the lowercase-hex res-9 cell, matching ``h3r9_crime_counts`` /
    ``h3r9_geogs``) plus the ``map_simple_use`` class (Residential / Non Residential / Mixed Use), so a
    consumer joins the per-class counts straight onto those by ``spatial_id``. Each building is placed by
    its footprint *centroid*: the ``buildings`` extract already tags every footprint with its res-9 cell
    (``h3r9_id``), so this just reads that column. Output is restricted to cells that appear in
    ``h3r9_crime_counts`` so the count grid lines up with the crime grid. No-op if the buildings table is
    absent. Only resolution 9 is ever produced — the extract carries a single ``h3r9_id``.
    """
    if not table_exists(con, BUILDINGS_TABLE):
        return
    con.execute(f"""
        {create_clause("TABLE", relation(h3_key(RESOLUTION), DATASET), replace=replace)} AS
        SELECT h3r9_id AS spatial_id, map_simple_use, COUNT(*) AS building_count
        FROM {BUILDINGS_TABLE}
        WHERE h3r9_id IN (SELECT spatial_id FROM h3r{RESOLUTION}_crime_counts)
        GROUP BY h3r9_id, map_simple_use;
    """)


def outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    if not table_exists(con, BUILDINGS_TABLE):
        return []
    return [relation(h3_key(RESOLUTION), DATASET)]


def build_hotspots(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Create ``hotspots_building_counts`` counting buildings per hotspot hex / ``map_simple_use``.

    The hexes don't line up with the H3 grid, so each footprint is placed by its centroid (the point the
    extract's ``h3r9_id`` also comes from) rather than reusing that column. No restriction to the crime
    grid is needed: the hotspot hexes *are* the grid. No-op if either input is absent.
    """
    if not (table_exists(con, BUILDINGS_TABLE) and hotspots.available(con)):
        return
    con.execute(f"""
        {create_clause("TABLE", relation(hotspots.HOTSPOT_UNIT.key, DATASET), replace=replace)} AS
        SELECT spatial_id, map_simple_use, COUNT(*) AS building_count
        FROM ({hotspots.placed_points(BUILDINGS_TABLE, "s.map_simple_use", point="ST_Centroid(s.geom)")})
        GROUP BY spatial_id, map_simple_use;
    """)


def hotspot_outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    if not (table_exists(con, BUILDINGS_TABLE) and hotspots.available(con)):
        return []
    return [relation(hotspots.HOTSPOT_UNIT.key, DATASET)]


STEP = TransformStep(
    name="building_counts",
    build=build,
    outputs=outputs,
    grid=Grid.H3,
    description="Buildings counted per resolution-9 H3 cell and map_simple_use, keyed by spatial_id.",
    depends_on=("crime_counts",),
    extract_inputs=(BUILDINGS_TABLE,),
)
