"""``{grid}_retail_centre_lookup`` — each cell's nearest retail centre (within a radius) + distance."""

import duckdb

from safer_streets_tooling.transform.base import TransformStep, create_clause, table_exists
from safer_streets_tooling.transform.grids import grids

# retail centres (CDRC Retail Centre Boundaries): unlike the overlap layers, each cell is matched
# to its *nearest* centre within RETAIL_RADIUS metres, folded into {grid}_geogs as scalar
# retail_centre_id + retail_centre_distance columns. Absent if the table was not loaded.
RETAIL_CENTRES_TABLE = "retail_centres"
RETAIL_RADIUS = 2000  # metres


def build(con: duckdb.DuckDBPyConnection, resolutions: list[int], replace: bool) -> None:
    """Create ``{grid}_retail_centre_lookup`` views: each cell's nearest retail centre.

    For every cell the closest retail centre within ``RETAIL_RADIUS`` metres is kept, with its
    distance; cells with none get NULLs (so there is exactly one row per cell). No-op if the
    retail_centres table is absent.
    """
    if not table_exists(con, RETAIL_CENTRES_TABLE):
        return
    for grid in grids(con, resolutions):
        con.execute(f"""
            {create_clause("VIEW", f"{grid.key}_retail_centre_lookup", replace=replace)} AS
            WITH cells AS {grid.cells}
            SELECT
                cells.spatial_id,
                rc.rc_id AS retail_centre_id,
                ST_Distance(cells.cell_geom, rc.geom) AS distance
            FROM cells
            LEFT JOIN {RETAIL_CENTRES_TABLE} rc ON ST_DWithin(cells.cell_geom, rc.geom, {RETAIL_RADIUS})
            QUALIFY ROW_NUMBER() OVER (PARTITION BY cells.spatial_id ORDER BY distance) = 1;
        """)


def outputs(con: duckdb.DuckDBPyConnection, resolutions: list[int]) -> list[str]:
    if not table_exists(con, RETAIL_CENTRES_TABLE):
        return []
    return [f"{grid.key}_retail_centre_lookup" for grid in grids(con, resolutions)]


STEP = TransformStep(
    name="retail_centre_lookups",
    build=build,
    outputs=outputs,
    description="Per-cell lookup of each H3 / BEAHIV cell's nearest retail centre (within 2km) + distance.",
    depends_on=("crime_counts", "beahiv_counts"),
    extract_inputs=(RETAIL_CENTRES_TABLE,),
)
