"""``crime_counts_beahiv_{side}`` — crimes counted per BEAHIV hexagonal cell / crime type / month.

The same schema and exclusions as ``crime_counts_h3_{res}`` (see :mod:`.crime_counts`), on the
equal-area hexagonal grid from the ``beahiv`` package instead of H3. The default 202 m side length
gives a cell of ~0.106 km², within a percent of an H3 resolution-9 cell, so the two grids are
directly comparable.

Cell ids are assigned by a **vectorised DuckDB UDF** (``type="arrow"``) over beahiv's ``bng_to_cell``,
which takes and returns pyarrow arrays: each call receives a whole 2048-row vector and returns the
encoded ids, so the per-row Python cost that a scalar UDF would pay is amortised across the vector.
"""

import duckdb
import numpy as np
import pyarrow as pa
from beahiv import bng_to_cell
from beahiv.cell_id import INVALID_CELL_ID
from duckdb.sqltypes import DOUBLE, UBIGINT

from safer_streets_tooling.transform.base import TransformStep, create_clause
from safer_streets_tooling.transform.base import register_udf as _register_udf
from safer_streets_tooling.transform.crime_counts import CRIME_FILTER
from safer_streets_tooling.transform.grids import ORIENTATION, SIDE_LENGTH

_UDF_NAME = "beahiv_cell_from_bng"


def _cell_from_bng(x: pa.ChunkedArray, y: pa.ChunkedArray) -> pa.Array:
    """Encode a vector of BNG (x, y) metres as BEAHIV cell ids.

    A pyarrow array in gives a ``uint64`` pyarrow array back, so the DuckDB vectors pass straight
    through to beahiv and back with no conversion here.

    It takes projected coordinates rather than lat/lon, for two reasons in order of importance:

    1. ``latlon_to_cell`` reprojects with pyproj, and calling pyproj from DuckDB's worker threads
       **segfaults the process** (reproducible on the full crime extract; survives only at
       ``threads = 1``, and neither a lock nor a thread-local ``Transformer`` avoids it). The
       transform phase runs with ``threads = 4``, so that path is unusable here.
    2. ``crime_data.geom`` is already BNG — projected once in the extractor — so going via lat/lon
       would reproject coordinates we already hold, at roughly double the cost.

    Verified equivalent: identical cell ids to ``latlon_to_cell`` on 2M rows of the extract.
    """
    return bng_to_cell(x, y, SIDE_LENGTH, ORIENTATION)


def register_udf(con: duckdb.DuckDBPyConnection) -> None:
    """Register the cell-encoding UDF on ``con`` (idempotent — see :func:`.base.register_udf`)."""
    _register_udf(con, _UDF_NAME, _cell_from_bng, [DOUBLE, DOUBLE], UBIGINT)


def table_name(side_length: int = SIDE_LENGTH) -> str:
    """This step's output table — at the default side length, ``grids.BEAHIV_GRID.counts_table``."""
    return f"crime_counts_beahiv_{side_length}"


def build(con: duckdb.DuckDBPyConnection, resolutions: list[int], replace: bool) -> None:
    """Create ``crime_counts_beahiv_{side}`` counting crimes per BEAHIV cell / crime type / month.

    ``spatial_id`` is the cell id as zero-padded lowercase hex — a string, like the H3 tables'
    ``lower(hex(...))`` cell ids, so consumers keep one ``spatial_id`` type across every grid (the
    raw ids exceed ``int64`` and would otherwise force an unsigned column). ``int(spatial_id, 16)``
    recovers the id beahiv's ``decode`` takes. ``resolutions`` is ignored — the grid is parameterised
    by ``SIDE_LENGTH`` in metres, not by an H3 resolution.

    Exclusions are ``crime_counts``' :data:`~.crime_counts.CRIME_FILTER` verbatim (un-geolocated and
    British Transport Police), so this grid counts exactly the crimes the H3 grids do and the two are
    comparable cell for cell. Every retained crime lands in exactly one cell, so the counts must sum
    back to the filtered input row count; a mismatch raises rather than emitting a skewed grid.

    A crime whose BNG coordinates fall far outside Great Britain would overflow the q/r bit budget;
    beahiv raises in that case rather than silently wrapping the cell id. A *missing* coordinate is
    the quiet failure instead — beahiv encodes NaN to ``INVALID_CELL_ID`` — so unencodable rows are
    checked for explicitly: the filter should already have excluded them, and if one gets through it
    is a bogus cell rather than a dropped crime, which conservation alone would not catch.
    """
    register_udf(con)
    name = table_name()
    expected = con.execute(f"SELECT COUNT(*) FROM crime_data WHERE {CRIME_FILTER}").fetchone()[0]  # ty:ignore[not-subscriptable]
    con.execute(f"""
        {create_clause("TABLE", name, replace=replace)} AS
        SELECT
            lpad(lower(hex({_UDF_NAME}(ST_X(geom), ST_Y(geom)))), 16, '0') AS spatial_id,
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
        f"SELECT COALESCE(SUM(count), 0) FROM {name} WHERE spatial_id IS NULL OR spatial_id = ?",
        [f"{INVALID_CELL_ID:016x}"],
    ).fetchone()[0]  # ty:ignore[not-subscriptable]
    if invalid:
        raise ValueError(
            f"{name}: {invalid:,} crimes did not encode to a cell (missing BNG coordinates) — they "
            f"passed the filter but have no usable geometry"
        )


def outputs(con: duckdb.DuckDBPyConnection, resolutions: list[int]) -> list[str]:
    return [table_name()]


def cell_ids(spatial_ids: list[str]) -> np.ndarray:
    """The uint64 cell ids behind a list of ``spatial_id`` hex strings (for beahiv's batch decoders)."""
    return np.array([int(s, 16) for s in spatial_ids], dtype=np.uint64)


STEP = TransformStep(
    name="beahiv_counts",
    build=build,
    outputs=outputs,
    description=f"Crimes counted per BEAHIV {SIDE_LENGTH}m-side flat hexagonal cell / crime_type / month (BTP excluded).",
    extract_inputs=("crime_data",),
)
