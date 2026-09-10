"""``h3r{res}_road_intersection_counts`` / ``hotspots_road_intersection_counts`` — road intersections counted per cell."""

import duckdb

from safer_streets_tooling.transform import hotspots
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


def build(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Create ``h3r{res}_road_intersection_counts`` counting road intersections per H3 cell.

    Keyed by ``spatial_id`` (the lowercase-hex cell, matching ``h3r{res}_crime_counts`` /
    ``h3r{res}_geogs``), so a consumer joins the count straight onto those by ``spatial_id`` — a
    companion to the ``road_overlap_length`` (total road length) already carried in ``h3r{res}_geogs``.
    Each intersection (an OS Open Roads junction/roundabout node, EPSG:27700) is placed by transforming
    its point back to WGS-84 and taking its cell at each resolution. Output is restricted to cells that
    appear in ``h3r{res}_crime_counts`` so the count grid lines up with the crime / road-length grid.
    No-op if the road_intersections table is absent.
    """
    if not table_exists(con, ROAD_INTERSECTIONS_TABLE):
        return
    for res in H3_RESOLUTIONS:
        con.execute(f"""
            {create_clause("TABLE", relation(h3_key(res), DATASET), replace=replace)} AS
            WITH cells AS (
                SELECT lower(hex(h3_latlng_to_cell(ST_Y(pt), ST_X(pt), {res}))) AS spatial_id
                FROM (
                    SELECT ST_Transform(geom, 'EPSG:27700', 'EPSG:4326', always_xy := true) AS pt
                    FROM {ROAD_INTERSECTIONS_TABLE}
                )
            )
            SELECT spatial_id, COUNT(*) AS road_intersection_count
            FROM cells
            WHERE spatial_id IN (SELECT spatial_id FROM h3r{res}_crime_counts)
            GROUP BY spatial_id;
        """)


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
