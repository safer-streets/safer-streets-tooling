"""Concurrent transform phase: run each H3 aggregation step as an :class:`AsyncNode` in an
:class:`AsyncPipeline`.

Every :class:`~safer_streets_tooling.transform.base.TransformStep` becomes a node keyed by its name;
``depends_on`` becomes graph edges, so the independent per-cell lookups (geographies, overlap layers,
nearest retail centre) run concurrently off ``crime_counts`` while ``geogs`` waits for all three. Each
step runs on its own ``con.cursor()`` so concurrent steps don't collide on the single in-memory
connection. ``AsyncNode.__call__`` turns any exception into an ``Err``, so the pipeline never aborts
mid-flight; ``build_all`` inspects the results afterwards and re-raises the first failure.

Mirrors ``safer_streets_tooling.extract.pipeline``: the extract phase does the same for ``Dataset``
entries, with each (blocking) step run in a worker thread.
"""

import asyncio
from collections.abc import Sequence
from pathlib import Path

import duckdb
from safer_streets_core.database import read_geoparquet, write_geoparquet

from safer_streets_tooling.async_node import AsyncNode
from safer_streets_tooling.async_pipeline import AsyncPipeline
from safer_streets_tooling.result import Ok, Result
from safer_streets_tooling.transform.base import H3_RESOLUTIONS, TransformStep


class TransformNode(AsyncNode[None, None]):
    """Pipeline node that builds one :class:`TransformStep` against the shared DuckDB (in a worker thread).

    Each node owns the parquet for the relations its step produces (``step.outputs``), mirroring how
    ``DatasetExtractNode`` owns its dataset parquet. When ``tdir`` is given and every output parquet is
    already present (and ``rebuild`` is False), the build is skipped and the cached parquet are loaded
    back as tables instead — so a downstream node (e.g. ``geogs``) can still read this step's relations
    from the in-memory catalog. Otherwise the step is built and each output is written out as parquet.

    All DB work runs on this node's own ``con.cursor()`` so concurrent steps don't collide on a single
    connection (the cursors share the one in-memory catalog). With ``tdir=None`` nothing is cached or
    written — the relations are only created in ``con`` (used for standalone, in-memory builds).
    Raised exceptions are captured as ``Err`` by ``AsyncNode.__call__``.
    """

    def __init__(
        self,
        step: TransformStep,
        con: duckdb.DuckDBPyConnection,
        resolutions: list[int],
        tdir: Path | None,
        *,
        replace: bool = True,
        rebuild: bool = False,
    ) -> None:
        self._step = step
        self._con = con
        self._resolutions = resolutions
        self._tdir = tdir
        self._replace = replace
        self._rebuild = rebuild
        super().__init__(*step.depends_on)

    async def execute(self, **deps: Result[None]) -> Result[None]:
        await asyncio.to_thread(self._run)
        return Ok(None)

    def _run(self) -> None:
        cur = self._con.cursor()
        names = self._step.outputs(cur, self._resolutions) if self._tdir is not None else []
        paths = {n: self._tdir / f"{n}.parquet" for n in names} if self._tdir is not None else {}

        if names and not self._rebuild and all(p.exists() for p in paths.values()):
            for name, path in paths.items():
                cur.execute(f'CREATE OR REPLACE TABLE "{name}" AS {read_geoparquet(path)}')
            print(f"[transform] {self._step.name}: cached output kept ({len(names)} relation(s))")
            return

        self._step.build(cur, self._resolutions, self._replace)
        for name, path in paths.items():
            write_geoparquet(cur, f'SELECT * FROM "{name}"', path)
        if names:
            print(f"[transform] {self._step.name}: built {len(names)} relation(s)")


def build_pipeline(
    steps: Sequence[TransformStep],
    con: duckdb.DuckDBPyConnection,
    *,
    resolutions: list[int] = H3_RESOLUTIONS,
    replace: bool = True,
    rebuild: bool = False,
    tdir: Path | None = None,
    verbose: bool = False,
) -> AsyncPipeline:
    """Wire ``steps`` into an :class:`AsyncPipeline`; ``depends_on`` become the graph edges.

    When ``tdir`` is given, each node caches its outputs there: a node whose output parquet all already
    exist is skipped (reloaded from disk) unless ``rebuild`` is True. With ``tdir=None`` the relations
    are built in ``con`` only (no caching)."""
    pipeline = AsyncPipeline(verbose=verbose)
    for step in steps:
        pipeline.add(step.name, TransformNode(step, con, resolutions, tdir, replace=replace, rebuild=rebuild))
    return pipeline


def build_all(
    steps: Sequence[TransformStep],
    con: duckdb.DuckDBPyConnection,
    *,
    resolutions: list[int] = H3_RESOLUTIONS,
    replace: bool = True,
    rebuild: bool = False,
    tdir: Path | None = None,
    verbose: bool = False,
) -> None:
    """Run all ``steps`` as an :class:`AsyncPipeline` over the shared connection ``con``.

    The independent lookup steps run concurrently (each on its own ``con.cursor()``); ``geogs`` waits for
    them. As in ``extract.run_extract``, ``AsyncNode.__call__`` captures any exception as ``Err`` so the
    pipeline never aborts mid-flight; each node's result is then unwrapped here, re-raising the first
    failure. When ``tdir`` is given, a node whose output parquet already exist is skipped unless
    ``rebuild`` is True (see :class:`TransformNode`); each built node writes its outputs to ``tdir``.
    When ``replace`` is False, existing tables/views are left untouched (``CREATE ... IF NOT EXISTS``)
    rather than rebuilt (``CREATE OR REPLACE``).
    """
    pipeline = build_pipeline(
        steps, con, resolutions=resolutions, replace=replace, rebuild=rebuild, tdir=tdir, verbose=verbose
    )
    asyncio.run(pipeline())
    for node_id in pipeline.nodes:
        pipeline[node_id].unwrap()  # re-raise the first captured exception, if any
