"""Tests for the concurrent extract phase (AsyncPipeline wiring)."""

import asyncio

import pytest

from safer_streets_tooling import build_db
from safer_streets_tooling.async_node import AsyncNode
from safer_streets_tooling.async_pipeline import AsyncPipeline
from safer_streets_tooling.datasets.base import Dataset, ExtractContext
from safer_streets_tooling.extract import build_pipeline, run_extract
from safer_streets_tooling.result import Err, Ok


def _ctx(tmp_path):
    return ExtractContext(staging=tmp_path)


def test_dependency_runs_before_dependent(tmp_path):
    """A dataset's extractor runs only after every depends_on extractor has finished."""
    order: list[str] = []

    def make(name):
        def extract(ctx):
            order.append(name)
            ctx.parquet(name).write_bytes(b"x")

        return extract

    roads = Dataset(name="open_roads", table="open_roads", extract=make("open_roads"))
    schools = Dataset(name="schools", table="schools", extract=make("schools"), depends_on=("open_roads",))

    # register dependent first to prove ordering is driven by the graph, not insertion order
    run_extract([schools, roads], _ctx(tmp_path), rebuild=False)
    assert order.index("open_roads") < order.index("schools")


def test_only_subset_drops_edges_to_absent_deps(tmp_path):
    """With a target subset, a depends_on outside the set is not a graph edge (assumed on disk)."""
    schools = Dataset(name="schools", table="schools", extract=lambda ctx: None, depends_on=("open_roads",))
    pipeline = build_pipeline([schools], _ctx(tmp_path), rebuild=True)
    assert pipeline.nodes["schools"].dependency_ids == ()  # open_roads not in the target set → no edge


def test_cached_parquet_skipped_unless_rebuild(tmp_path):
    calls: list[str] = []

    def extract(ctx):
        calls.append("ran")
        ctx.parquet("d").write_bytes(b"x")

    ds = Dataset(name="d", table="d", extract=extract)
    run_extract([ds], _ctx(tmp_path), rebuild=False)  # absent → runs
    run_extract([ds], _ctx(tmp_path), rebuild=False)  # present → skipped
    assert calls == ["ran"]
    run_extract([ds], _ctx(tmp_path), rebuild=True)  # forced → re-runs
    assert calls == ["ran", "ran"]


def test_optional_failure_skipped_required_propagates(tmp_path):
    def boom(ctx):
        raise RuntimeError("nope")

    run_extract([Dataset(name="opt", table="opt", extract=boom)], _ctx(tmp_path), rebuild=False)  # swallowed

    required = Dataset(name="req", table="req", extract=boom, optional=False)
    with pytest.raises(RuntimeError, match="nope"):
        run_extract([required], _ctx(tmp_path), rebuild=False)


def test_async_node_captures_exception_as_err():
    class Boom(AsyncNode[None, None]):
        async def execute(self, **kwargs):
            raise ValueError("boom")

    result = asyncio.run(Boom()())
    assert isinstance(result, Err) and result.is_err()
    assert "boom" in repr(result.error)


def test_async_pipeline_passes_dependency_results():
    """A node receives its dependency's Result as a kwarg named after the dependency."""

    class Source(AsyncNode[None, int]):
        async def execute(self, **kwargs):
            return Ok(21)

    seen: dict[str, int] = {}

    class Doubler(AsyncNode[int, int]):
        async def execute(self, *, src):  # ty:ignore[invalid-method-override]
            seen["got"] = src.unwrap()
            return Ok(src.unwrap() * 2)

    pipeline = AsyncPipeline()
    pipeline.add("src", Source())
    pipeline.add("doubler", Doubler())  # depends_on inferred from the `src` kwonly arg
    asyncio.run(pipeline())

    assert seen["got"] == 21
    assert pipeline["doubler"].unwrap() == 42


def test_run_extract_exposed_on_build_db():
    # build_db re-exports run_extract so the CLI and tests share one entry point
    assert build_db.run_extract is run_extract
