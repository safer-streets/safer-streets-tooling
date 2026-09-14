"""``beahiv202_*_counts`` — the per-cell counts, aggregated onto the BEAHIV hexagonal grid.

The same schema and exclusions as ``h3r{res}_crime_counts`` (see :mod:`.crime_counts`), on the
equal-area hexagonal grid described in :mod:`.beahiv` instead of H3. The BEAHIV counterpart of
``hotspot_counts`` — except that placing a crime needs no spatial join: a cell id is arithmetic on the
crime's BNG coordinates, so this is the H3 mechanism with beahiv's encoder in place of
``h3_latlng_to_cell``, and the same exact-conservation guarantee follows.

The crime counts are built here because the encoder and their conservation checks are specific to this
grid; the other four (street lights, buildings, population, road intersections) are the same measures
the H3 grid carries, so each lives with its H3 counterpart (``building_counts.build_beahiv`` and
friends) and this module only wires them in — as ``hotspot_counts`` does for the hexes. Each counts
every cell holding a feature, as its H3 counterpart does, so the two grids cover the same features.
"""

import duckdb
from beahiv import INVALID_CELL_ID

from safer_streets_tooling.beahiv_grid import SIDE_LENGTH
from safer_streets_tooling.transform import (
    beahiv,
    building_counts,
    population_counts,
    road_intersection_counts,
    streetlight_counts,
)
from safer_streets_tooling.transform.base import Grid, TransformStep, create_clause
from safer_streets_tooling.transform.crime_counts import CRIME_FILTER, expected_crimes

# the modules whose counts have a BEAHIV equivalent; each exposes build_beahiv / beahiv_outputs
_MODULES = (streetlight_counts, building_counts, population_counts, road_intersection_counts)


def build(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Create ``beahiv202_crime_counts`` counting crimes per BEAHIV cell / crime type / month.

    The grid is parameterised by a side length in metres, not by an H3 resolution. ``spatial_id`` is the
    cell id as a ``BIGINT`` (see :mod:`.beahiv`), the same column ``beahiv202`` and ``beahiv202_geogs``
    are keyed by.

    Exclusions are ``crime_counts``' :data:`~.crime_counts.CRIME_FILTER` verbatim (un-geolocated and
    British Transport Police), so this grid counts exactly the crimes the H3 grids do and the two are
    comparable cell for cell. Every retained crime lands in exactly one cell, so the counts must sum
    back to the filtered input row count; a mismatch raises rather than emitting a skewed grid.

    A crime whose BNG coordinates fall far outside Great Britain would overflow the q/r bit budget;
    beahiv raises in that case rather than silently wrapping the cell id. A *missing* coordinate is
    the quiet failure instead — beahiv encodes NaN to its ``INVALID_CELL_ID`` sentinel — so unencodable
    rows are checked for explicitly: the filter should already have excluded them, and if one gets
    through it is a bogus cell rather than a dropped crime, which conservation alone would not catch.
    """
    beahiv.register_udfs(con)
    name = beahiv.COUNTS_TABLE
    expected = expected_crimes(con)
    con.execute(f"""
        {create_clause("TABLE", name, replace=replace)} AS
        SELECT
            {beahiv.ENCODE_UDF}(ST_X(geom), ST_Y(geom)) AS spatial_id,
            crime_type,
            _month AS month,
            COUNT(*) AS count
        FROM crime_data
        WHERE {CRIME_FILTER}
        GROUP BY spatial_id, crime_type, month;
    """)
    actual = con.execute(f"SELECT COALESCE(SUM(count), 0) FROM {name}").fetchone()[0]  # ty:ignore[not-subscriptable]
    if actual != expected:
        raise ValueError(
            f"{name}: counted {actual:,} crimes but {expected:,} input rows passed the filter — the "
            f"per-cell counts are not conserved (aggregation dropped or duplicated crimes)"
        )

    invalid = con.execute(
        f"SELECT COALESCE(SUM(count), 0) FROM {name} WHERE spatial_id IS NULL OR spatial_id = {INVALID_CELL_ID}"
    ).fetchone()[0]  # ty:ignore[not-subscriptable]
    if invalid:
        raise ValueError(
            f"{name}: {invalid:,} crimes did not encode to a cell (missing BNG coordinates) — they "
            f"passed the filter but have no usable geometry"
        )
    cells = con.execute(f"SELECT COUNT(DISTINCT spatial_id) FROM {name}").fetchone()[0]  # ty:ignore[not-subscriptable]
    print(f"  {name}: {actual:,} crimes in {cells:,} cells")

    # the crime counts are this grid's cells, so they must exist before the others restrict to them
    for module in _MODULES:
        module.build_beahiv(con, replace)


def outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [beahiv.COUNTS_TABLE] + [name for module in _MODULES for name in module.beahiv_outputs(con)]


STEP = TransformStep(
    name="beahiv_counts",
    build=build,
    outputs=outputs,
    grid=Grid.BEAHIV,
    description=f"Crime / street light / building / population / road-intersection counts per BEAHIV {SIDE_LENGTH}m-side hexagonal cell, keyed by spatial_id.",
    extract_inputs=(
        "crime_data",
        "streetlights",
        "buildings",
        "workplace_population",
        "residential_population",
        "road_intersections",
    ),
)
