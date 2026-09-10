"""``h3r9_streetlight_counts`` / ``hotspots_streetlight_counts`` — street lights counted per cell."""

import duckdb

from safer_streets_tooling.grids import BEAHIV_ID, H3_ID
from safer_streets_tooling.transform import beahiv, hotspots
from safer_streets_tooling.transform.base import Grid, TransformStep, create_clause, h3_key, relation, table_exists

STREETLIGHTS_TABLE = "streetlights"
DATASET = "streetlight_counts"
RESOLUTION = 9


def _build_on(con: duckdb.DuckDBPyConnection, unit_key: str, cell: str, replace: bool) -> None:
    """Create ``{unit_key}_streetlight_counts`` from the cell id the extract tags on each light.

    Both the H3 and BEAHIV grids tag their cell onto the light, so this is a group-and-count on that
    column for either; only the hotspot hexes need a spatial join (see :func:`build_hotspots`).

    Every cell holding a light is counted, on either grid — see :func:`.building_counts._build_on`.
    """
    con.execute(f"""
        {create_clause("TABLE", relation(unit_key, DATASET), replace=replace)} AS
        SELECT {cell} AS spatial_id, COUNT(*) AS streetlight_count
        FROM {STREETLIGHTS_TABLE}
        WHERE {cell} IS NOT NULL
        GROUP BY {cell};
    """)


def build(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Create ``h3r9_streetlight_counts``. No-op if the streetlights table is absent."""
    if not table_exists(con, STREETLIGHTS_TABLE):
        return
    _build_on(con, h3_key(RESOLUTION), H3_ID, replace)


def build_beahiv(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Create ``beahiv202_streetlight_counts``. No-op until the extract carries ``beahiv202_id``."""
    if not beahiv.tagged(con, STREETLIGHTS_TABLE):
        return
    _build_on(con, beahiv.BEAHIV_UNIT.key, BEAHIV_ID, replace)


def beahiv_outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    if not beahiv.tagged(con, STREETLIGHTS_TABLE):
        return []
    return [relation(beahiv.BEAHIV_UNIT.key, DATASET)]


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
