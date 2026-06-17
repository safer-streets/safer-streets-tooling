"""Tests for the H3 transform phase (AsyncPipeline wiring), mirroring test_extract_pipeline."""

import asyncio

import duckdb
import pytest
from safer_streets_core.database import duckdb_connector, write_geoparquet

from safer_streets_tooling.transform import STEPS, TransformNode, TransformStep, build_all, build_pipeline


def _connect():
    """A writable in-memory connection, or skip the test if the spatial extensions can't be fetched."""
    try:
        return duckdb_connector(writeable=True)
    except duckdb.HTTPException as e:  # extension download unavailable
        pytest.skip(f"extension download unavailable: {e}")


def _step(name, build, *, outputs=lambda con, res: [], depends_on=()):
    return TransformStep(name=name, build=build, outputs=outputs, depends_on=depends_on)


def test_pipeline_wires_data_dependencies():
    """crime_counts has no deps; the three lookups depend on it; geogs waits for all three."""
    con = duckdb.connect()
    pipeline = build_pipeline(STEPS, con, resolutions=[8])

    assert pipeline.nodes["crime_counts"].dependency_ids == ()
    assert pipeline.nodes["geo_lookups"].dependency_ids == ("crime_counts",)
    assert pipeline.nodes["overlap_lookups"].dependency_ids == ("crime_counts",)
    assert pipeline.nodes["retail_centre_lookups"].dependency_ids == ("crime_counts",)
    assert pipeline.nodes["geogs"].dependency_ids == ("geo_lookups", "overlap_lookups", "retail_centre_lookups")


def test_steps_run_respecting_dependency_order():
    """build_all runs crime_counts before every lookup, and every lookup before geogs."""
    order: list[str] = []

    def record(name):
        def build(con, resolutions, replace):
            order.append(name)

        return build

    steps = [
        _step("crime_counts", record("crime_counts")),
        _step("geo_lookups", record("geo_lookups"), depends_on=("crime_counts",)),
        _step("overlap_lookups", record("overlap_lookups"), depends_on=("crime_counts",)),
        _step("retail_centre_lookups", record("retail_centre_lookups"), depends_on=("crime_counts",)),
        _step("geogs", record("geogs"), depends_on=("geo_lookups", "overlap_lookups", "retail_centre_lookups")),
    ]

    build_all(steps, duckdb.connect(), resolutions=[8])

    assert order.index("crime_counts") < order.index("geo_lookups")
    assert order.index("crime_counts") < order.index("overlap_lookups")
    assert order.index("crime_counts") < order.index("retail_centre_lookups")
    assert order.index("geo_lookups") < order.index("geogs")
    assert order.index("overlap_lookups") < order.index("geogs")
    assert order.index("retail_centre_lookups") < order.index("geogs")


def test_step_failure_is_reraised():
    """A failing transform step is captured as Err by the node, then re-raised by build_all."""

    def boom(con, resolutions, replace):
        raise RuntimeError("nope")

    steps = [_step("boom", boom)]

    with pytest.raises(RuntimeError, match="nope"):
        build_all(steps, duckdb.connect(), resolutions=[8])


def test_node_builds_and_writes_output_parquet(tmp_path):
    """With a tdir and no cached parquet, the node builds and writes each declared output."""
    con = _connect()
    calls = []

    def build(cur, resolutions, replace):
        calls.append("built")
        cur.execute('CREATE TABLE "foo" AS SELECT 1 AS spatial_id, 2 AS v')

    node = TransformNode(_step("n", build, outputs=lambda con, res: ["foo"]), con, [8], tmp_path)
    asyncio.run(node())

    assert calls == ["built"]
    assert (tmp_path / "foo.parquet").exists()


def test_node_skips_build_and_reloads_cached_parquet(tmp_path):
    """When the output parquet exists (and rebuild is False) the build is skipped and the parquet is
    reloaded as a table so downstream nodes can still read it."""
    seed = _connect()
    write_geoparquet(seed, "SELECT 1 AS spatial_id, 99 AS v", tmp_path / "foo.parquet")
    seed.close()

    con = _connect()
    calls = []

    def build(cur, resolutions, replace):
        calls.append("built")

    node = TransformNode(_step("n", build, outputs=lambda con, res: ["foo"]), con, [8], tmp_path)
    asyncio.run(node())

    assert calls == []  # cached → build skipped
    assert con.execute('SELECT v FROM "foo"').fetchone()[0] == 99  # reloaded into the catalog


def test_node_rebuild_ignores_cache(tmp_path):
    """rebuild=True rebuilds even when the output parquet already exists."""
    seed = _connect()
    write_geoparquet(seed, "SELECT 1 AS spatial_id, 99 AS v", tmp_path / "foo.parquet")
    seed.close()

    con = _connect()
    calls = []

    def build(cur, resolutions, replace):
        calls.append("built")
        cur.execute('CREATE TABLE "foo" AS SELECT 1 AS spatial_id, 2 AS v')

    node = TransformNode(_step("n", build, outputs=lambda con, res: ["foo"]), con, [8], tmp_path, rebuild=True)
    asyncio.run(node())

    assert calls == ["built"]
