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

from collections.abc import Callable
from dataclasses import dataclass, field

import duckdb

H3_RESOLUTIONS = [8, 9, 10]


@dataclass(frozen=True)
class TransformStep:
    """One H3 aggregation step in the transform pipeline.

    ``build(con, resolutions, replace)`` creates the step's relations; ``outputs(con, resolutions)``
    returns the relation names it produces (used to cache them as parquet and skip rebuilds).
    ``depends_on`` lists the names of steps whose relations this one reads. ``extract_inputs`` lists the
    extract dataset names this step reads (their parquet live in the extract dir); together with the
    output parquet of its ``depends_on`` steps they are the step's inputs for staleness checks — the
    cached output is reused only when it exists *and* is newer than every input.
    """

    name: str
    build: Callable[[duckdb.DuckDBPyConnection, list[int], bool], None]
    outputs: Callable[[duckdb.DuckDBPyConnection, list[int]], list[str]]
    depends_on: tuple[str, ...] = field(default_factory=tuple)
    extract_inputs: tuple[str, ...] = field(default_factory=tuple)


def create_clause(kind: str, name: str, *, replace: bool) -> str:
    """Build the leading CREATE clause for a table or view.

    replace=True  -> ``CREATE OR REPLACE {kind} {name}``    (always rebuilt)
    replace=False -> ``CREATE {kind} IF NOT EXISTS {name}`` (kept if it already exists)
    """
    return f"CREATE OR REPLACE {kind} {name}" if replace else f"CREATE {kind} IF NOT EXISTS {name}"


def table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    return (
        con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ? AND table_schema = 'main'",
            [name],
        ).fetchone()[0]  # ty:ignore[not-subscriptable]
        > 0
    )
