"""``{unit}_retail_centre_lookup`` — each cell's nearest retail centre (within a radius) + distance."""

import duckdb

from safer_streets_tooling.transform.base import SpatialUnit, TransformStep, create_clause, h3_unit, table_exists

# retail centres (CDRC Retail Centre Boundaries): unlike the overlap layers, each cell is matched
# to its *nearest* centre within RETAIL_RADIUS metres, folded into the unit's geogs as scalar
# retail_centre_id + retail_centre_distance columns. Absent if the table was not loaded.
RETAIL_CENTRES_TABLE = "retail_centres"
RETAIL_RADIUS = 2000  # metres


def build_unit(con: duckdb.DuckDBPyConnection, unit: SpatialUnit, replace: bool) -> None:
    """Create the ``{unit.key}_retail_centre_lookup`` view: each cell's nearest retail centre.

    For every cell the closest retail centre within ``RETAIL_RADIUS`` metres is kept, with its
    distance; cells with none get NULLs (so there is exactly one row per cell). No-op if the
    retail_centres table is absent.
    """
    if not table_exists(con, RETAIL_CENTRES_TABLE):
        return
    con.execute(f"""
        {create_clause("VIEW", f"{unit.key}_retail_centre_lookup", replace=replace)} AS
        WITH cells AS ({unit.cells})
        SELECT
            cells.spatial_id,
            rc.rc_id AS retail_centre_id,
            ST_Distance(cells.cell_geom, rc.geom) AS distance
        FROM cells
        LEFT JOIN {RETAIL_CENTRES_TABLE} rc ON ST_DWithin(cells.cell_geom, rc.geom, {RETAIL_RADIUS})
        QUALIFY ROW_NUMBER() OVER (PARTITION BY cells.spatial_id ORDER BY distance) = 1;
    """)


def unit_outputs(con: duckdb.DuckDBPyConnection, unit: SpatialUnit) -> list[str]:
    if not table_exists(con, RETAIL_CENTRES_TABLE):
        return []
    return [f"{unit.key}_retail_centre_lookup"]


def build(con: duckdb.DuckDBPyConnection, resolutions: list[int], replace: bool) -> None:
    for res in resolutions:
        build_unit(con, h3_unit(res), replace)


def outputs(con: duckdb.DuckDBPyConnection, resolutions: list[int]) -> list[str]:
    return [name for res in resolutions for name in unit_outputs(con, h3_unit(res))]


STEP = TransformStep(
    name="retail_centre_lookups",
    build=build,
    outputs=outputs,
    description="Per-cell lookup of each H3 cell's nearest retail centre (within 2km) + distance.",
    depends_on=("crime_counts",),
    extract_inputs=(RETAIL_CENTRES_TABLE,),
)
