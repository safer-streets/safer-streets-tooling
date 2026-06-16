"""Concurrent extract phase: run each dataset's extractor as an :class:`AsyncNode` in an
:class:`AsyncPipeline`.

Every dataset becomes a node keyed by its name; ``depends_on`` becomes graph edges, so independent
downloads run concurrently while ``schools`` still waits for ``open_roads`` and ``imd`` for
``local_authority_districts``. Each (blocking) extractor runs in a worker thread via
``asyncio.to_thread`` so the downloads/DuckDB work overlap. ``AsyncNode.__call__`` turns any exception
into an ``Err`` result, so the pipeline never aborts mid-flight; ``run_extract`` inspects the results
afterwards and re-raises only for *required* datasets.
"""

import asyncio
from pathlib import Path

from safer_streets_tooling.async_node import AsyncNode
from safer_streets_tooling.async_pipeline import AsyncPipeline
from safer_streets_tooling.datasets import Dataset, ExtractContext
from safer_streets_tooling.result import Ok, Result


class DatasetExtractNode(AsyncNode[Path | None, Path | None]):
    """Pipeline node that extracts one dataset to its parquet (in a worker thread).

    Returns ``Ok(parquet_path)`` when the parquet is present afterwards, ``Ok(None)`` if the extractor
    produced nothing (e.g. an optional source was empty). Raised exceptions are captured as ``Err`` by
    ``AsyncNode.__call__``. Dependency results are accepted as ``**deps`` purely to sequence execution;
    extractors read upstream parquet from disk via ``ctx``.
    """

    def __init__(self, dataset: Dataset, ctx: ExtractContext, dep_ids: tuple[str, ...], *, rebuild: bool) -> None:
        self._dataset = dataset
        self._ctx = ctx
        self._rebuild = rebuild
        super().__init__(*dep_ids)

    async def execute(self, **deps: Result[Path | None]) -> Result[Path | None]:
        out = self._ctx.parquet(self._dataset.name)
        if out.exists() and not self._rebuild:
            print(f"[extract] {self._dataset.name}: cached parquet kept")
            return Ok(out)
        print(f"[extract] {self._dataset.name}…")
        await asyncio.to_thread(self._dataset.extract, self._ctx)
        return Ok(out if out.exists() else None)


def build_pipeline(
    targets: list[Dataset], ctx: ExtractContext, *, rebuild: bool, verbose: bool = False
) -> AsyncPipeline:
    """Wire ``targets`` into an AsyncPipeline. Only ``depends_on`` edges whose dependency is *also* in
    ``targets`` become graph edges; a dependency outside the target set (e.g. ``--only schools`` when
    ``open_roads`` was extracted earlier) is assumed already present on disk."""
    target_names = {ds.name for ds in targets}
    pipeline = AsyncPipeline(verbose=verbose)
    for ds in targets:
        dep_ids = tuple(d for d in ds.depends_on if d in target_names)
        pipeline.add(ds.name, DatasetExtractNode(ds, ctx, dep_ids, rebuild=rebuild))
    return pipeline


def run_extract(targets: list[Dataset], ctx: ExtractContext, *, rebuild: bool, verbose: bool = False) -> None:
    """Extract ``targets`` concurrently. When ``rebuild`` is False, datasets whose parquet already
    exists are kept; when True they are re-extracted. After the run, an optional dataset's failure is
    reported and skipped; a required dataset's failure is re-raised."""
    print(f"\n=== Extracting {len(targets)} dataset(s) → {ctx.staging} ===\n")
    pipeline = build_pipeline(targets, ctx, rebuild=rebuild, verbose=verbose)
    asyncio.run(pipeline())

    for ds in targets:
        result = pipeline[ds.name]
        if result.is_err():
            if not ds.optional:
                result.unwrap()  # re-raises the captured exception
            print(f"  Skipping {ds.name}: {result.error}")
