"""``crime_counts_beahiv_{side}`` — crimes counted per BEAHIV hexagonal cell / crime type / month.

The same schema and exclusions as ``crime_counts_h3_{res}`` (see :mod:`.crime_counts`), on the
equal-area hexagonal grid from the ``beahiv`` package instead of H3. The default 202 m side length
gives a cell of ~0.106 km², within a percent of an H3 resolution-9 cell, so the two grids are
directly comparable.

Cell ids are assigned by a **vectorised DuckDB UDF** (``type="arrow"``) wrapping beahiv's batch
encoder: each call receives a whole 2048-row vector as pyarrow arrays and returns the encoded cell
ids, so the per-row Python cost that a scalar UDF would pay is amortised across the vector.
"""

import duckdb
import numpy as np
import pyarrow as pa
from beahiv import Orientation, bng_to_cell
from duckdb.sqltypes import DOUBLE, UBIGINT

from safer_streets_tooling.transform.base import TransformStep, create_clause
from safer_streets_tooling.transform.crime_counts import CRIME_FILTER

SIDE_LENGTH = 202
ORIENTATION = Orientation.FLAT

_UDF_NAME = "beahiv_cell_from_bng"


def _cell_from_bng(x: pa.ChunkedArray, y: pa.ChunkedArray) -> pa.Array:
    """Encode a vector of BNG (x, y) metres as BEAHIV cell ids.

    Passing numpy arrays dispatches ``bng_to_cell`` to its vectorised overload — one call per DuckDB
    vector, not per row.

    It takes projected coordinates rather than lat/lon, for two reasons in order of importance:

    1. ``latlon_to_cell`` reprojects with pyproj, and calling pyproj from DuckDB's worker threads
       **segfaults the process** (reproducible on the full crime extract; survives only at
       ``threads = 1``, and neither a lock nor a thread-local ``Transformer`` avoids it). The
       transform phase runs with ``threads = 4``, so that path is unusable here.
    2. ``crime_data.geom`` is already BNG — projected once in the extractor — so going via lat/lon
       would reproject coordinates we already hold, at roughly double the cost.

    Verified equivalent: identical cell ids to ``latlon_to_cell`` on 2M rows of the extract.

    ``combine_chunks`` because DuckDB hands each argument over as a ``ChunkedArray`` while numpy
    wants one contiguous buffer.
    """
    xs = x.combine_chunks().to_numpy(zero_copy_only=False)
    ys = y.combine_chunks().to_numpy(zero_copy_only=False)
    return pa.array(bng_to_cell(xs, ys, SIDE_LENGTH, ORIENTATION), type=pa.uint64())


def register_udf(con: duckdb.DuckDBPyConnection) -> None:
    """Register the cell-encoding UDF on ``con``, unless the catalog already has it.

    UDFs live in the catalog, which cursors share and which outlives a single ``build`` — so a
    rebuild (or a second step on another cursor) would otherwise fail on the name already existing.
    The catalog is the thing to test: ``remove_function`` and re-create looks like the obvious way to
    make this idempotent, but once the UDF has *executed* over real data it only deregisters the
    Python side — ``duckdb_functions()`` still lists the name and ``create_function`` then raises
    ``CatalogException``. Skipping the re-registration is safe here because the encoding is fixed by
    ``SIDE_LENGTH`` / ``ORIENTATION``, so an existing registration is by definition the same function.
    """
    sql = "SELECT COUNT(*) FROM duckdb_functions() WHERE function_name = ?"
    registered = con.execute(sql, [_UDF_NAME]).fetchone()[0]  # ty:ignore[not-subscriptable]
    if not registered:
        con.create_function(_UDF_NAME, _cell_from_bng, [DOUBLE, DOUBLE], UBIGINT, type="arrow")


def table_name(side_length: int = SIDE_LENGTH) -> str:
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
    ``encode_batch`` raises in that case rather than silently wrapping the cell id.
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
