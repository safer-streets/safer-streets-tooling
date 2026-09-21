"""Tests for the concurrent extract phase (AsyncPipeline wiring)."""

import asyncio
import os
from datetime import datetime
from types import SimpleNamespace
from zipfile import ZipFile

import duckdb
import pandas as pd
import pytest
import requests

from safer_streets_tooling import data_pipeline
from safer_streets_tooling.async_node import AsyncNode
from safer_streets_tooling.async_pipeline import AsyncPipeline
from safer_streets_tooling.extract import (
    build_pipeline,
    crime,
    residential_population,
    run_extract,
    workplace_population,
)
from safer_streets_tooling.extract.base import Dataset, ExtractContext
from safer_streets_tooling.result import Err, Ok


def _ctx(tmp_path):
    return ExtractContext(staging=tmp_path)


def _unreachable(*args, **kwargs):
    """A network call the test asserts is never made."""
    raise AssertionError("the network was contacted")


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


class TestCrimeArchiveCurrency:
    """The cached police.uk archive is reused only while it is still the published release.

    Two independent signals, and both must agree before 1.7GB is downloaded again: the calendar says
    a release is *due* (a 15th has passed since the file was written), and the server says one has
    actually *happened* (``latest.zip`` redirects to a later month than the cache holds).
    """

    def _archive(self, tmp_path, month, mtime):
        """A stand-in archive holding data up to ``month``, written at ``mtime``."""
        path = tmp_path / "police_uk_crime_data_latest.zip"
        with ZipFile(path, "w") as z:
            z.writestr(f"{month}/avon-and-somerset-street.csv", "Longitude,Latitude\n")
        os.utime(path, (mtime.timestamp(), mtime.timestamp()))
        return path

    def _head(self, monkeypatch, month):
        """Stub the HEAD so it answers with the 302 to ``month``'s archive, as police.uk does."""
        location = f"https://policeuk-data.s3.amazonaws.com/archive/{month}.zip"
        monkeypatch.setattr(crime.requests, "head", lambda *a, **kw: SimpleNamespace(headers={"location": location}))

    @pytest.mark.parametrize(
        ("now", "due"),
        [
            (datetime(2026, 9, 14), False),  # 17 Aug → 14 Sep: no 15th has passed
            (datetime(2026, 9, 15), True),  # …→ 15 Sep: this month's release is due
            (datetime(2026, 8, 20), False),  # written after the 15th of its own month
            (datetime(2027, 1, 3), True),  # months behind; the step back crosses a year end
        ],
    )
    def test_release_due_gates_on_the_15th(self, now, due):
        assert crime._release_due(datetime(2026, 8, 17, 13, 57).timestamp(), now) is due

    def test_fresh_download_is_not_rechecked(self, tmp_path, monkeypatch):
        """No 15th since the download means no network call at all — the usual run costs nothing."""
        archive = self._archive(tmp_path, "2026-07", datetime.now())
        monkeypatch.setattr(crime.requests, "head", _unreachable)
        assert crime._is_stale("https://data.police.uk/data/archive/latest.zip", archive) is False

    def test_superseded_archive_is_stale(self, tmp_path, monkeypatch):
        """The 17 Aug file: a 15th has passed and the server now serves a later month."""
        archive = self._archive(tmp_path, "2026-06", datetime(2026, 8, 17))
        self._head(monkeypatch, "2026-07")
        assert crime._is_stale("https://data.police.uk/data/archive/latest.zip", archive) is True

    def test_overdue_but_unreleased_archive_is_kept(self, tmp_path, monkeypatch):
        """A release is due but hasn't landed: the server still names the cached month, so no download.

        This is why the date is only a gate — releases slip, and the calendar alone would throw away
        a perfectly current archive.
        """
        archive = self._archive(tmp_path, "2026-06", datetime(2026, 8, 17))
        self._head(monkeypatch, "2026-06")
        assert crime._is_stale("https://data.police.uk/data/archive/latest.zip", archive) is False

    def test_unreachable_server_keeps_the_cache(self, tmp_path, monkeypatch):
        """A failed check is no evidence of a new release; the build goes on with what it has."""
        archive = self._archive(tmp_path, "2026-06", datetime(2026, 8, 17))

        def boom(*args, **kwargs):
            raise requests.ConnectionError("no route to host")

        monkeypatch.setattr(crime.requests, "head", boom)
        assert crime._is_stale("https://data.police.uk/data/archive/latest.zip", archive) is False

    def test_unreadable_archive_is_stale(self, tmp_path, monkeypatch):
        """A file that won't open as a zip is unusable, so it is replaced without asking the server."""
        archive = tmp_path / "police_uk_crime_data_latest.zip"
        archive.write_bytes(b"not a zip")
        stale = datetime(2026, 8, 17).timestamp()
        os.utime(archive, (stale, stale))
        monkeypatch.setattr(crime.requests, "head", _unreachable)
        assert crime._is_stale("https://data.police.uk/data/archive/latest.zip", archive) is True


def test_run_extract_exposed_on_data_pipeline():
    # data_pipeline re-exports run_extract so the CLI and tests share one entry point
    assert data_pipeline.run_extract is run_extract
