"""``h3_{res}_retail_centre_lookup`` — each H3 cell's nearest retail centre (within a radius) + distance."""

import duckdb

from safer_streets_tooling.transform.base import TransformStep, create_clause, table_exists

# retail centres (CDRC Retail Centre Boundaries): unlike the overlap layers, each cell is matched
# to its *nearest* centre within RETAIL_RADIUS metres, folded into h3_geogs as scalar
# retail_centre_id + retail_centre_distance columns. Absent if the table was not loaded.
RETAIL_CENTRES_TABLE = "retail_centres"
RETAIL_RADIUS = 2000  # metres


def build(con: duckdb.DuckDBPyConnection, resolutions: list[int], replace: bool) -> None:
    """Create ``h3_{res}_retail_centre_lookup`` views: each cell's nearest retail centre.

    For every H3 cell the closest retail centre within ``RETAIL_RADIUS`` metres is kept, with its
    distance; cells with none get NULLs (so there is exactly one row per cell). No-op if the
    retail_centres table is absent.
    """
    if not table_exists(con, RETAIL_CENTRES_TABLE):
        return
    for res in resolutions:
        con.execute(f"""
            {create_clause("VIEW", f"h3_{res}_retail_centre_lookup", replace=replace)} AS
            WITH cells AS (
                SELECT
                    spatial_id,
                    ST_Transform(
                        ST_GeomFromText(h3_cell_to_boundary_wkt(spatial_id)),
                        'EPSG:4326', 'EPSG:27700', always_xy := true
                    ) AS cell_geom
                FROM (SELECT DISTINCT spatial_id FROM crime_counts_h3_{res})
            )
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
    return [f"h3_{res}_retail_centre_lookup" for res in resolutions]


STEP = TransformStep(name="retail_centre_lookups", build=build, outputs=outputs, depends_on=("crime_counts",))
