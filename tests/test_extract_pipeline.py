"""Tests for the concurrent extract phase (AsyncPipeline wiring)."""

import asyncio
from zipfile import ZipFile

import duckdb
import pandas as pd
import pytest

from safer_streets_tooling import data_pipeline
from safer_streets_tooling.async_node import AsyncNode
from safer_streets_tooling.async_pipeline import AsyncPipeline
from safer_streets_tooling.extract import build_pipeline, residential_population, run_extract, workplace_population
from safer_streets_tooling.extract.base import Dataset, ExtractContext
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


def test_workplace_population_extract_reads_oa_csv_from_zip(tmp_path, monkeypatch):
    """The extractor unpacks the OA-level WP001 CSV from the (mocked) nomis zip and writes the parquet
    keyed by spatial_id."""

    def fake_download(url, dest):
        with ZipFile(dest, "w") as z:
            z.writestr("WP001_oa.csv", "Output Areas Code,Count\nE00000001,76\nW00000002,10\n")

    monkeypatch.setattr(workplace_population, "download", fake_download)
    monkeypatch.setattr(workplace_population, "raw_dir", lambda: tmp_path)

    try:
        workplace_population.extract(_ctx(tmp_path))
    except duckdb.HTTPException as e:  # extension download unavailable
        pytest.skip(f"extension download unavailable: {e}")

    df = pd.read_parquet(tmp_path / "workplace_population.parquet")
    assert list(df.columns) == ["spatial_id", "workplace_population"]
    assert dict(zip(df["spatial_id"], df["workplace_population"], strict=True)) == {"E00000001": 76, "W00000002": 10}


def test_residential_population_extract_pivots_restypes(tmp_path, monkeypatch):
    """The extractor fetches the (mocked) nomis TS001 CSV and pivots one row per (OA, residence type)
    into one row per OA with household/communal columns."""

    def fake_download(url, dest):
        assert "c2021_restype_3=1,2" in url and "uid=k" in url
        dest.write_text(
            "GEOGRAPHY_CODE,C2021_RESTYPE_3,C2021_RESTYPE_3_NAME,OBS_VALUE\n"
            "E00000001,1,Lives in a household,90\n"
            "E00000001,2,Lives in a communal establishment,10\n"
            "W00000002,1,Lives in a household,50\n"
            "W00000002,2,Lives in a communal establishment,0\n"
        )

    monkeypatch.setattr(residential_population, "download", fake_download)
    monkeypatch.setattr(residential_population, "raw_dir", lambda: tmp_path)
    monkeypatch.setattr(residential_population, "api_key", lambda: {"uid": "k"})
    monkeypatch.setattr(residential_population, "EXPECTED_OA_COUNT", 2)

    try:
        residential_population.extract(_ctx(tmp_path))
    except duckdb.HTTPException as e:  # extension download unavailable
        pytest.skip(f"extension download unavailable: {e}")

    df = pd.read_parquet(tmp_path / "residential_population.parquet").set_index("spatial_id")
    assert list(df.columns) == ["household_population", "communal_population"]
    assert list(df.loc["E00000001"]) == [90, 10]
    assert list(df.loc["W00000002"]) == [50, 0]


def test_residential_population_truncated_response_raises(tmp_path, monkeypatch):
    """An unexpected OA count (a truncated nomis response) must raise rather than write a partial parquet."""

    def fake_download(url, dest):
        dest.write_text("GEOGRAPHY_CODE,C2021_RESTYPE_3,C2021_RESTYPE_3_NAME,OBS_VALUE\nE00000001,1,household,90\n")

    monkeypatch.setattr(residential_population, "download", fake_download)
    monkeypatch.setattr(residential_population, "raw_dir", lambda: tmp_path)
    monkeypatch.setattr(residential_population, "api_key", lambda: {"uid": "k"})

    try:
        with pytest.raises(RuntimeError, match="truncated"):
            residential_population.extract(_ctx(tmp_path))
    except duckdb.HTTPException as e:  # extension download unavailable
        pytest.skip(f"extension download unavailable: {e}")
    assert not (tmp_path / "residential_population.parquet").exists()


def test_residential_population_missing_api_key_raises(tmp_path, monkeypatch):
    """Without NOMIS_API_KEY the extractor raises with registration guidance (the optional dataset is
    then skipped by the pipeline rather than aborting the build)."""

    def no_key():
        raise KeyError("NOMIS_API_KEY")

    monkeypatch.setattr(residential_population, "api_key", no_key)
    with pytest.raises(RuntimeError, match="NOMIS_API_KEY"):
        residential_population.extract(_ctx(tmp_path))


def test_run_extract_exposed_on_data_pipeline():
    # data_pipeline re-exports run_extract so the CLI and tests share one entry point
    assert data_pipeline.run_extract is run_extract
