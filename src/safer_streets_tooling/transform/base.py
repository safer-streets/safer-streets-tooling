"""Step primitives shared by the H3 transform pipeline.

A :class:`TransformStep` describes one H3 aggregation step: how to *build* its relations against a
DuckDB connection, which relation names it *outputs* (so the pipeline can cache/skip it), and which
other steps it ``depends_on``. The registry in ``safer_streets_tooling.transform`` lists the steps in
dependency order and the pipeline wires them into an ``AsyncPipeline`` — mirroring how the extract
phase turns ``Dataset`` entries into nodes.

The transforms operate on an open, writable DuckDB connection that already contains a ``crime_data``
table (street-level crimes) and one boundary table per ONS geography (each with a ``spatial_id`` code
and a BNG ``geom`` column). Ported from the ``duckdb-spatial`` prototype notebook (safer-streets-eda).
"""

import threading
from collections.abc import Callable
from dataclasses import dataclass, field

import duckdb
from duckdb.sqltypes import DuckDBPyType

H3_RESOLUTIONS = [8, 9, 10]


@dataclass(frozen=True)
class TransformStep:
    """One H3 aggregation step in the transform pipeline.

    ``build(con, resolutions, replace)`` creates the step's relations; ``outputs(con, resolutions)``
    returns the relation names it produces (used to cache them as parquet and skip rebuilds).
    ``description`` is a one-line human summary of the relation(s) the step produces, surfaced in the
    ``index.parquet`` catalogue (keep it current when the outputs change). ``depends_on`` lists the names
    of steps whose relations this one reads. ``extract_inputs`` lists the extract dataset names this step
    reads (their parquet live in the extract dir); together with the output parquet of its ``depends_on``
    steps they are the step's inputs for staleness checks — the cached output is reused only when it
    exists *and* is newer than every input.
    """

    name: str
    build: Callable[[duckdb.DuckDBPyConnection, list[int], bool], None]
    outputs: Callable[[duckdb.DuckDBPyConnection, list[int]], list[str]]
    description: str = ""
    depends_on: tuple[str, ...] = field(default_factory=tuple)
    extract_inputs: tuple[str, ...] = field(default_factory=tuple)


def create_clause(kind: str, name: str, *, replace: bool) -> str:
    """Build the leading CREATE clause for a table or view.

    replace=True  -> ``CREATE OR REPLACE {kind} {name}``    (always rebuilt)
    replace=False -> ``CREATE {kind} IF NOT EXISTS {name}`` (kept if it already exists)
    """
    return f"CREATE OR REPLACE {kind} {name}" if replace else f"CREATE {kind} IF NOT EXISTS {name}"


# UDFs live in the catalog, which every cursor shares, so concurrent steps race to register the same
# name. Serialise the check-then-create so only one of them wins.
_UDF_LOCK = threading.Lock()


def register_udf(
    con: duckdb.DuckDBPyConnection,
    name: str,
    fn: Callable[..., object],
    params: list[DuckDBPyType],
    return_type: DuckDBPyType,
) -> None:
    """Register a vectorised (``type="arrow"``) Python UDF on ``con``, unless the catalog already has it.

    The catalog outlives a single ``build`` and is shared by every cursor, so a rebuild — or a second
    step registering the same helper on another cursor — would otherwise fail on the name already
    existing. The catalog is the thing to test: ``remove_function`` and re-create looks like the obvious
    way to make this idempotent, but once the UDF has *executed* over real data it only deregisters the
    Python side — ``duckdb_functions()`` still lists the name and ``create_function`` then raises
    ``CatalogException``. Skipping the re-registration is safe because these UDFs are pure functions of
    module-level constants, so an existing registration is by definition the same function.
    """
    with _UDF_LOCK:
        sql = "SELECT COUNT(*) FROM duckdb_functions() WHERE function_name = ?"
        if not con.execute(sql, [name]).fetchone()[0]:  # ty:ignore[not-subscriptable]
            con.create_function(name, fn, params, return_type, type="arrow")


def table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    return (
        con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ? AND table_schema = 'main'",
            [name],
        ).fetchone()[0]  # ty:ignore[not-subscriptable]
        > 0
    )
