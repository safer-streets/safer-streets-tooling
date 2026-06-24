"""``building_counts_h3_9`` — buildings counted per resolution-9 H3 cell and ``map_simple_use``."""

import duckdb

from safer_streets_tooling.transform.base import TransformStep, create_clause, table_exists

BUILDINGS_TABLE = "buildings"
RESOLUTION = 9


def build(con: duckdb.DuckDBPyConnection, resolutions: list[int], replace: bool) -> None:
    """Create ``building_counts_h3_9`` counting buildings per resolution-9 H3 cell / ``map_simple_use``.

    Keyed by ``spatial_id`` (the lowercase-hex res-9 cell, matching ``crime_counts_h3_9`` /
    ``h3_9_geogs``) plus the ``map_simple_use`` class (Residential / Non Residential / Mixed Use), so a
    consumer joins the per-class counts straight onto those by ``spatial_id``. Each building is placed by
    its footprint *centroid*: the BNG geometry is reprojected to WGS-84 and indexed to an H3 cell with
    ``h3_latlng_to_cell`` (the same expression ``crime_counts`` uses). Output is restricted to cells that
    appear in ``crime_counts_h3_9`` so the count grid lines up with the crime grid. No-op if the buildings
    table is absent. ``resolutions`` is ignored — this is only produced at resolution 9.
    """
    if not table_exists(con, BUILDINGS_TABLE):
        return
    con.execute(f"""
        {create_clause("TABLE", f"building_counts_h3_{RESOLUTION}", replace=replace)} AS
        WITH cells AS (
            SELECT
                lower(hex(h3_latlng_to_cell(ST_Y(centroid), ST_X(centroid), {RESOLUTION}))) AS spatial_id,
                map_simple_use
            FROM (
                SELECT
                    ST_Transform(ST_Centroid(geom), 'EPSG:27700', 'EPSG:4326', always_xy := true) AS centroid,
                    map_simple_use
                FROM {BUILDINGS_TABLE}
            )
        )
        SELECT spatial_id, map_simple_use, COUNT(*) AS building_count
        FROM cells
        WHERE spatial_id IN (SELECT spatial_id FROM crime_counts_h3_{RESOLUTION})
        GROUP BY spatial_id, map_simple_use;
    """)


def outputs(con: duckdb.DuckDBPyConnection, resolutions: list[int]) -> list[str]:
    if not table_exists(con, BUILDINGS_TABLE):
        return []
    return [f"building_counts_h3_{RESOLUTION}"]


STEP = TransformStep(
    name="building_counts",
    build=build,
    outputs=outputs,
    depends_on=("crime_counts",),
    extract_inputs=(BUILDINGS_TABLE,),
)
