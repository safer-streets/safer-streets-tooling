"""``building_workplace_population`` — the OA workplace population allocated to non-residential buildings.

The workplace population (Census 2021 WP001) is an estimate of the usually resident population aged 16
years and over, working in an area. It includes people who work mainly at or from home, or do not have
a fixed place of work, in their area of usual residence. It is published per output area; this step
disaggregates each OA's count to the buildings where people plausibly work.
"""

import duckdb

from safer_streets_tooling.transform.base import TransformStep, create_clause, table_exists

BUILDINGS_TABLE = "buildings"
WORKPLACE_TABLE = "workplace_population"
OUTPUT_TABLE = "building_workplace_population"

# map_simple_use -> allocation weight multiplier. Residential buildings get none of the workplace
# population (the home-worker share of WP001 has no per-building signal to allocate it by), so they are
# simply excluded; a Mixed Use building counts at half the weight of a purely commercial one.
USE_WEIGHTS = {"Non Residential": 1.0, "Mixed Use": 0.5}


def _has_size_columns(con: duckdb.DuckDBPyConnection) -> bool:
    """True when the buildings table carries the size columns the allocation weights need (they were
    added to the extract later, so a cached parquet may predate them — re-extract to pick them up)."""
    cols = {
        row[0]
        for row in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ? AND table_schema = 'main'",
            [BUILDINGS_TABLE],
        ).fetchall()
    }
    return {"gross_area", "premise_area"} <= cols


def _ready(con: duckdb.DuckDBPyConnection) -> bool:
    if not (table_exists(con, BUILDINGS_TABLE) and table_exists(con, WORKPLACE_TABLE)):
        return False
    if not _has_size_columns(con):
        print(
            f"  [building_workplace] {BUILDINGS_TABLE} lacks the size columns (gross_area/premise_area); "
            f"re-extract buildings to build {OUTPUT_TABLE} — skipping"
        )
        return False
    return True


def build(con: duckdb.DuckDBPyConnection, resolutions: list[int], replace: bool) -> None:
    """Create ``building_workplace_population``: each OA's WP001 count shared across its non-residential
    buildings.

    Within each OA (buildings are tagged with ``oa21cd`` by the extract), every Non Residential or Mixed
    Use building gets a weight of its total floor area (``gross_area``, falling back to the footprint
    ``premise_area`` where the floor count is unknown) times its USE_WEIGHTS multiplier (commercial 1.0,
    mixed 0.5; Residential excluded). The OA's workplace population is then split pro rata to the
    weights, keyed by ``verisk_premise_id`` and carrying ``oa21cd`` + the building's res-9 ``h3_9_id``
    so the estimates aggregate straight onto the crime grid. An OA whose workplace population has no
    eligible building to land on is omitted (its count is not reallocated elsewhere). No-op if either
    input table is absent, or if the buildings table predates the size columns.
    """
    if not _ready(con):
        return
    use_weights = ", ".join(f"('{use}', {weight})" for use, weight in USE_WEIGHTS.items())
    con.execute(f"""
        {create_clause("TABLE", OUTPUT_TABLE, replace=replace)} AS
        WITH weighted AS (
            SELECT
                b.verisk_premise_id, b.oa21cd, b.h3_9_id,
                COALESCE(b.gross_area, b.premise_area) * w.weight AS weight
            FROM {BUILDINGS_TABLE} b
            JOIN (VALUES {use_weights}) w(map_simple_use, weight) ON b.map_simple_use = w.map_simple_use
            WHERE b.oa21cd IS NOT NULL
        )
        SELECT
            wt.verisk_premise_id, wt.oa21cd, wt.h3_9_id,
            wp.workplace_population * wt.weight / SUM(wt.weight) OVER (PARTITION BY wt.oa21cd)
                AS workplace_population
        FROM weighted wt
        JOIN {WORKPLACE_TABLE} wp ON wp.spatial_id = wt.oa21cd;
    """)


def outputs(con: duckdb.DuckDBPyConnection, resolutions: list[int]) -> list[str]:
    if not (table_exists(con, BUILDINGS_TABLE) and table_exists(con, WORKPLACE_TABLE) and _has_size_columns(con)):
        return []
    return [OUTPUT_TABLE]


STEP = TransformStep(
    name="building_workplace",
    build=build,
    outputs=outputs,
    description="Census 2021 OA workplace population allocated to non-residential buildings pro rata to floor area × use weight.",
    extract_inputs=(BUILDINGS_TABLE, WORKPLACE_TABLE),
)
