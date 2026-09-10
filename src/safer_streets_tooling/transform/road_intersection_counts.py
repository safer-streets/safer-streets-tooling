"""``h3r{res}_road_intersection_counts`` / ``hotspots_road_intersection_counts`` — road intersections counted per cell."""

import duckdb

from safer_streets_tooling.beahiv_grid import cell_id_sql
from safer_streets_tooling.transform import beahiv, hotspots
from safer_streets_tooling.transform.base import (
    H3_RESOLUTIONS,
    Grid,
    TransformStep,
    create_clause,
    h3_key,
    relation,
    table_exists,
)

ROAD_INTERSECTIONS_TABLE = "road_intersections"
DATASET = "road_intersection_counts"


def _build_on(
    con: duckdb.DuckDBPyConnection, unit_key: str, cell: str, replace: bool, source: str | None = None
) -> None:
    """Create ``{unit_key}_road_intersection_counts``, placing each junction node by ``cell``.

    Unlike the other layers this one carries no cell id — the intersections are derived from the road
    network rather than extracted as a point layer — so the cell is computed here: an h3 lookup for the
    H3 grids (which needs the point back in WGS-84, hence ``source``), arithmetic on the BNG point for
    BEAHIV. Every cell holding an intersection is counted, on either grid — see
    :func:`.building_counts._build_on`.
    """
    con.execute(f"""
        {create_clause("TABLE", relation(unit_key, DATASET), replace=replace)} AS
        WITH cells AS (SELECT {cell} AS spatial_id FROM {source or ROAD_INTERSECTIONS_TABLE})
        SELECT spatial_id, COUNT(*) AS road_intersection_count
        FROM cells
        WHERE spatial_id IS NOT NULL
        GROUP BY spatial_id;
    """)


def build(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Create ``h3r{res}_road_intersection_counts`` counting road intersections per H3 cell.

    Keyed by ``spatial_id``, so a consumer joins the count straight onto the unit's counts / geogs — a
    companion to the ``road_overlap_length`` (total road length) those geogs already carry. Each
    intersection (an OS Open Roads junction/roundabout node, EPSG:27700) is placed by transforming its
    point back to WGS-84 and taking its cell. No-op if the road_intersections table is absent.
    """
    if not table_exists(con, ROAD_INTERSECTIONS_TABLE):
        return
    wgs84 = (
        f"(SELECT ST_Transform(geom, 'EPSG:27700', 'EPSG:4326', always_xy := true) AS pt "
        f"FROM {ROAD_INTERSECTIONS_TABLE})"
    )
    for res in H3_RESOLUTIONS:
        _build_on(con, h3_key(res), f"lower(hex(h3_latlng_to_cell(ST_Y(pt), ST_X(pt), {res})))", replace, wgs84)


def build_beahiv(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Create ``beahiv202_road_intersection_counts``: the nodes are already BNG, so the cell is
    arithmetic on the point rather than a reprojection. No-op if the grid or the layer is absent."""
    if not (beahiv.available(con) and table_exists(con, ROAD_INTERSECTIONS_TABLE)):
        return
    beahiv.register_udfs(con)
    _build_on(con, beahiv.BEAHIV_UNIT.key, cell_id_sql("geom"), replace)


def beahiv_outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    if not (beahiv.available(con) and table_exists(con, ROAD_INTERSECTIONS_TABLE)):
        return []
    return [relation(beahiv.BEAHIV_UNIT.key, DATASET)]


def outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    if not table_exists(con, ROAD_INTERSECTIONS_TABLE):
        return []
    return [relation(h3_key(res), DATASET) for res in H3_RESOLUTIONS]


def build_hotspots(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Create ``hotspots_road_intersection_counts`` counting road intersections per hotspot hex.

    The intersection nodes are already BNG points, so unlike the H3 counts (which transform back to
    WGS-84 to take a cell id) this is a plain point-in-polygon join. No restriction to the crime grid is
    needed: the hotspot hexes *are* the grid. No-op if either input is absent.
    """
    if not (table_exists(con, ROAD_INTERSECTIONS_TABLE) and hotspots.available(con)):
        return
    con.execute(f"""
        {create_clause("TABLE", relation(hotspots.HOTSPOT_UNIT.key, DATASET), replace=replace)} AS
        SELECT spatial_id, COUNT(*) AS road_intersection_count
        FROM ({hotspots.placed_points(ROAD_INTERSECTIONS_TABLE)})
        GROUP BY spatial_id;
    """)


def hotspot_outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    if not (table_exists(con, ROAD_INTERSECTIONS_TABLE) and hotspots.available(con)):
        return []
    return [relation(hotspots.HOTSPOT_UNIT.key, DATASET)]


STEP = TransformStep(
    name="road_intersection_counts",
    build=build,
    outputs=outputs,
    grid=Grid.H3,
    description="Road intersections (OS Open Roads junctions/roundabouts) counted per H3 cell, keyed by spatial_id.",
    depends_on=("crime_counts",),
    extract_inputs=(ROAD_INTERSECTIONS_TABLE,),
)
