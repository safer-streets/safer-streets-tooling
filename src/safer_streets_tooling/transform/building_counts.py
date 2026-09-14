"""``h3r9_building_counts`` / ``hotspots_building_counts`` — buildings counted per cell and ``map_simple_use``."""

import duckdb

from safer_streets_tooling.grids import BEAHIV_ID, H3_ID
from safer_streets_tooling.transform import beahiv, hotspots
from safer_streets_tooling.transform.base import Grid, TransformStep, create_clause, h3_key, relation, table_exists

BUILDINGS_TABLE = "buildings"
DATASET = "building_counts"
RESOLUTION = 9


def _build_on(con: duckdb.DuckDBPyConnection, unit_key: str, cell: str, replace: bool) -> None:
    """Create ``{unit_key}_building_counts`` from the cell id the extract already tags on each footprint.

    Both the H3 and BEAHIV grids tag their cell onto the building (``h3r9_id`` / ``beahiv202_id``), so
    counting is a group-and-count on that column whichever of them is asked for; only the hotspot hexes,
    which carry no id, need a spatial join (see :func:`build_hotspots`). Each building is placed by its
    footprint *centroid* — the point both id columns are derived from.

    Keyed by ``spatial_id`` plus the ``map_simple_use`` class (Residential / Non Residential / Mixed
    Use), so a consumer joins the per-class counts straight onto the unit's counts / geogs.

    Every cell holding a building is counted, not only those carrying crimes: the cell comes from the
    feature's own coordinates, so there is nothing to bound and the two grids stay comparable by
    covering the same features either way. The ``*_geogs`` tables are the crime cells alone, so a cell
    counted here without crimes has no attributes to join to — on both grids alike.
    """
    con.execute(f"""
        {create_clause("TABLE", relation(unit_key, DATASET), replace=replace)} AS
        SELECT {cell} AS spatial_id, map_simple_use, COUNT(*) AS building_count
        FROM {BUILDINGS_TABLE}
        WHERE {cell} IS NOT NULL
        GROUP BY {cell}, map_simple_use;
    """)


def build(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Create ``h3r9_building_counts``. No-op if the buildings table is absent. Only resolution 9 is
    ever produced — the extract carries a single ``h3r9_id``."""
    if not table_exists(con, BUILDINGS_TABLE):
        return
    _build_on(con, h3_key(RESOLUTION), H3_ID, replace)


def build_beahiv(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Create ``beahiv202_building_counts`` — the same counts on the BEAHIV grid, off its own id column.

    No-op until the buildings extract has been re-run to carry ``beahiv202_id`` (see
    :func:`.beahiv.tagged`).
    """
    if not beahiv.tagged(con, BUILDINGS_TABLE):
        return
    _build_on(con, beahiv.BEAHIV_UNIT.key, BEAHIV_ID, replace)


def beahiv_outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    if not beahiv.tagged(con, BUILDINGS_TABLE):
        return []
    return [relation(beahiv.BEAHIV_UNIT.key, DATASET)]


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
