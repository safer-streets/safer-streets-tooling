"""``population_counts_h3_9`` / ``population_counts_hotspots`` — residential + workplace population per cell.

The Census 2021 populations are published per output area: TS001 usual residents (the
``residential_population`` extract) and WP001 workplace population (the ``workplace_population``
extract — an estimate of the usually resident population aged 16 years and over, working in an area,
including people who work mainly at or from home, or do not have a fixed place of work, in their area
of usual residence). This step disaggregates both onto a grid via the buildings where people
plausibly live and work.
"""

import duckdb

from safer_streets_tooling.transform import hotspots
from safer_streets_tooling.transform.base import TransformStep, create_clause, table_exists

BUILDINGS_TABLE = "buildings"
WORKPLACE_TABLE = "workplace_population"
RESIDENTIAL_TABLE = "residential_population"
RESOLUTION = 9

# map_simple_use -> (workplace weight, residential weight) allocation multipliers: a purely commercial
# building takes only workplace population, a purely residential one only residents, and a Mixed Use
# building is treated as 50-50 (half weight in each pool).
USE_WEIGHTS = {
    "Non Residential": (1.0, 0.0),
    "Mixed Use": (0.5, 0.5),
    "Residential": (0.0, 1.0),
}


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
    if not all(table_exists(con, t) for t in (BUILDINGS_TABLE, WORKPLACE_TABLE, RESIDENTIAL_TABLE)):
        return False
    if not _has_size_columns(con):
        print(
            f"  [population_counts] {BUILDINGS_TABLE} lacks the size columns (gross_area/premise_area); "
            f"re-extract buildings to build population_counts_h3_{RESOLUTION} — skipping"
        )
        return False
    return True


def _allocation_sql(buildings_sql: str) -> str:
    """The allocation query, over a ``buildings_sql`` yielding one row per building: its ``spatial_id``
    (the cell it sits in, NULL if none), ``oa21cd``, ``map_simple_use`` and the two size columns.

    Each building's share of its OA is its total floor area (``gross_area``, falling back to the
    footprint ``premise_area`` where the floor count is unknown) times its USE_WEIGHTS multiplier,
    normalised within the OA; the per-building assignments are then summed per cell.

    ``buildings_sql`` must cover *every* building in the OA, including those the grid doesn't reach —
    the shares are normalised over whatever it returns, so filtering to a partial grid first would
    hand each cell the whole OA's population instead of its own share. Buildings outside the grid
    (NULL ``spatial_id``) are dropped after the shares are worked out.
    """
    use_weights = ", ".join(f"('{use}', {work}, {res})" for use, (work, res) in USE_WEIGHTS.items())
    return f"""
        WITH weighted AS (
            SELECT
                b.spatial_id, b.oa21cd,
                COALESCE(b.gross_area, b.premise_area) * w.work_weight AS work_weight,
                COALESCE(b.gross_area, b.premise_area) * w.res_weight AS res_weight
            FROM ({buildings_sql}) b
            JOIN (VALUES {use_weights}) w(map_simple_use, work_weight, res_weight)
            ON b.map_simple_use = w.map_simple_use
            WHERE b.oa21cd IS NOT NULL
        ),
        assigned AS (
            SELECT
                wt.spatial_id,
                rp.household_population + rp.communal_population AS oa_residential,
                wt.res_weight / NULLIF(SUM(wt.res_weight) OVER (PARTITION BY wt.oa21cd), 0) AS res_share,
                wp.workplace_population AS oa_workplace,
                wt.work_weight / NULLIF(SUM(wt.work_weight) OVER (PARTITION BY wt.oa21cd), 0) AS work_share
            FROM weighted wt
            LEFT JOIN {RESIDENTIAL_TABLE} rp ON rp.spatial_id = wt.oa21cd
            LEFT JOIN {WORKPLACE_TABLE} wp ON wp.spatial_id = wt.oa21cd
        )
        SELECT
            spatial_id,
            COALESCE(SUM(oa_residential * res_share), 0) AS residential_population,
            COALESCE(SUM(oa_workplace * work_share), 0) AS workplace_population
        FROM assigned
        WHERE spatial_id IS NOT NULL
        GROUP BY spatial_id
    """


def _report_allocated(con: duckdb.DuckDBPyConnection, table: str) -> None:
    """Print how much of each source total landed on the grid (see :func:`build` for what can't)."""
    res_alloc, work_alloc = con.execute(
        f"SELECT SUM(residential_population), SUM(workplace_population) FROM {table}"
    ).fetchone()  # ty:ignore[not-iterable]
    res_total, work_total = con.execute(
        f"SELECT (SELECT SUM(household_population + communal_population) FROM {RESIDENTIAL_TABLE}), "
        f"(SELECT SUM(workplace_population) FROM {WORKPLACE_TABLE})"
    ).fetchone()  # ty:ignore[not-iterable]
    print(
        f"  {table}: allocated {res_alloc:,.0f} of {res_total:,} residential "
        f"({res_alloc / res_total:.1%}) and {work_alloc:,.0f} of {work_total:,} workplace "
        f"({work_alloc / work_total:.1%}) population"
    )


def build(con: duckdb.DuckDBPyConnection, resolutions: list[int], replace: bool) -> None:
    """Create ``population_counts_h3_9``: the OA populations assigned to buildings, then summed per cell.

    Each building's share of its OA (buildings carry ``oa21cd`` from the extract) is its total floor
    area (``gross_area``, falling back to the footprint ``premise_area`` where the floor count is
    unknown) times its USE_WEIGHTS multiplier, normalised within the OA — one weighting per population:
    the workplace population goes to Non Residential (×1.0) and Mixed Use (×0.5) buildings, the
    residential population (households + communal establishments) to Residential (×1.0) and Mixed Use
    (×0.5). The per-building assignments are then grouped by the building's res-9 ``h3_9_id`` and
    summed, keyed by ``spatial_id`` to match ``crime_counts_h3_9`` / ``h3_9_geogs``.

    Both populations are conserved onto the grid except where they cannot be assigned: an OA with no
    building of the right type (its population has nowhere to land), and buildings whose centroid falls
    in no OA (they receive nothing). The allocated shares of the source totals are reported. No-op if
    any input table is absent, or if the buildings table predates the size columns.
    ``resolutions`` is ignored — this is only produced at resolution 9.

    The allocation itself lives in :func:`_allocation_sql`, shared with the hotspot-hex version.
    """
    if not _ready(con):
        return
    buildings = f"SELECT h3_9_id AS spatial_id, oa21cd, map_simple_use, gross_area, premise_area FROM {BUILDINGS_TABLE}"
    con.execute(f"""
        {create_clause("TABLE", f"population_counts_h3_{RESOLUTION}", replace=replace)} AS
        {_allocation_sql(buildings)};
    """)
    _report_allocated(con, f"population_counts_h3_{RESOLUTION}")


def outputs(con: duckdb.DuckDBPyConnection, resolutions: list[int]) -> list[str]:
    if not (
        all(table_exists(con, t) for t in (BUILDINGS_TABLE, WORKPLACE_TABLE, RESIDENTIAL_TABLE))
        and _has_size_columns(con)
    ):
        return []
    return [f"population_counts_h3_{RESOLUTION}"]


def build_hotspots(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Create ``population_counts_hotspots``: the same allocation summed per hotspot hex.

    Identical weighting (:func:`_allocation_sql`); only the cell a building belongs to differs — placed
    by footprint centroid into a hex rather than read from the extract's ``h3_9_id``. The placement is an
    *outer* join so buildings outside every hex still count towards their OA's shares (they just receive
    nothing); much less of each OA total therefore lands here than on the national H3 grid. No-op if any
    input is absent, or if the buildings table predates the size columns.
    """
    if not (_ready(con) and hotspots.available(con)):
        return
    buildings = hotspots.placed_points(
        BUILDINGS_TABLE,
        "s.oa21cd",
        "s.map_simple_use",
        "s.gross_area",
        "s.premise_area",
        point="ST_Centroid(s.geom)",
        outer=True,
    )
    con.execute(f"""
        {create_clause("TABLE", "population_counts_hotspots", replace=replace)} AS
        {_allocation_sql(buildings)};
    """)
    _report_allocated(con, "population_counts_hotspots")


def hotspot_outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    if not (
        all(table_exists(con, t) for t in (BUILDINGS_TABLE, WORKPLACE_TABLE, RESIDENTIAL_TABLE))
        and _has_size_columns(con)
        and hotspots.available(con)
    ):
        return []
    return ["population_counts_hotspots"]


STEP = TransformStep(
    name="population_counts",
    build=build,
    outputs=outputs,
    description="Census 2021 residential (TS001) + workplace (WP001) population per res-9 cell, allocated via buildings by floor area × use weight.",
    extract_inputs=(BUILDINGS_TABLE, WORKPLACE_TABLE, RESIDENTIAL_TABLE),
)
