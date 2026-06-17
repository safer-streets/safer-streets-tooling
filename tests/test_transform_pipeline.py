"""Tests for the H3 transform phase (AsyncPipeline wiring), mirroring test_extract_pipeline."""

import asyncio
import os

import duckdb
import pytest
from safer_streets_core.database import duckdb_connector, write_geoparquet

from safer_streets_tooling.transform import STEPS, TransformNode, TransformStep, build_all, build_pipeline, geogs
from safer_streets_tooling.transform.geo_lookups import GEOGRAPHY_MAPPINGS


def _connect():
    """A writable in-memory connection, or skip the test if the spatial extensions can't be fetched."""
    try:
        return duckdb_connector(writeable=True)
    except duckdb.HTTPException as e:  # extension download unavailable
        pytest.skip(f"extension download unavailable: {e}")


def _step(name, build, *, outputs=lambda con, res: [], depends_on=(), extract_inputs=()):
    return TransformStep(name=name, build=build, outputs=outputs, depends_on=depends_on, extract_inputs=extract_inputs)


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

    node = TransformNode(_step("n", build, outputs=lambda con, res: ["foo"]), [], con, [8], None, tmp_path)
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

    node = TransformNode(_step("n", build, outputs=lambda con, res: ["foo"]), [], con, [8], None, tmp_path)
    asyncio.run(node())

    assert calls == []  # exists, no inputs → fresh → build skipped
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

    node = TransformNode(
        _step("n", build, outputs=lambda con, res: ["foo"]), [], con, [8], None, tmp_path, rebuild=True
    )
    asyncio.run(node())

    assert calls == ["built"]


def test_node_rebuilds_when_input_is_newer(tmp_path):
    """A cached output older than one of its inputs is rebuilt (Make-style staleness)."""
    edir = tmp_path / "extract"
    tdir = tmp_path / "transform"
    edir.mkdir()
    tdir.mkdir()
    seed = _connect()
    write_geoparquet(seed, "SELECT 1 AS spatial_id, 1 AS v", tdir / "foo.parquet")  # output
    write_geoparquet(seed, "SELECT 1 AS x", edir / "bar.parquet")  # input
    seed.close()
    newer = (tdir / "foo.parquet").stat().st_mtime + 10  # input mtime > output mtime
    os.utime(edir / "bar.parquet", (newer, newer))

    con = _connect()
    calls = []

    def build(cur, resolutions, replace):
        calls.append("built")
        cur.execute('CREATE TABLE "foo" AS SELECT 1 AS spatial_id, 2 AS v')

    step = _step("n", build, outputs=lambda con, res: ["foo"], extract_inputs=("bar",))
    node = TransformNode(step, [], con, [8], edir, tdir)
    asyncio.run(node())

    assert calls == ["built"]  # input newer than output → stale → rebuilt


def test_node_keeps_cache_when_output_is_newer(tmp_path):
    """A cached output newer than all its inputs is reused (build skipped)."""
    edir = tmp_path / "extract"
    tdir = tmp_path / "transform"
    edir.mkdir()
    tdir.mkdir()
    seed = _connect()
    write_geoparquet(seed, "SELECT 1 AS x", edir / "bar.parquet")  # input
    write_geoparquet(seed, "SELECT 1 AS spatial_id, 99 AS v", tdir / "foo.parquet")  # output
    seed.close()
    newer = (edir / "bar.parquet").stat().st_mtime + 10  # output mtime > input mtime
    os.utime(tdir / "foo.parquet", (newer, newer))

    con = _connect()
    calls = []

    def build(cur, resolutions, replace):
        calls.append("built")

    step = _step("n", build, outputs=lambda con, res: ["foo"], extract_inputs=("bar",))
    node = TransformNode(step, [], con, [8], edir, tdir)
    asyncio.run(node())

    assert calls == []  # output newer than input → fresh → reused
    assert con.execute('SELECT v FROM "foo"').fetchone()[0] == 99


def test_geogs_folds_overlap_area_and_road_length():
    """h3_geogs carries the largest overlap area per area layer (greenspace, land cover) — summing would
    double-count overlapping polygons — and the total road length. Hand-built lookups (geogs.build is
    pure SQL)."""
    con = duckdb.connect()
    # source-table presence drives which overlap features / retail centres are folded in
    for table in ("open_greenspace", "land_cover", "open_roads", "retail_centres"):
        con.execute(f"CREATE TABLE {table}(x INTEGER)")
    # one row per ONS geography lookup for cell 'a'
    for key in GEOGRAPHY_MAPPINGS:
        con.execute(f"CREATE TABLE h3_8_{key}_lookup AS SELECT 'a' AS spatial_id, 'X' AS {key}")
    # two greenspace polygons (largest 10), one land-cover polygon (7), two road segments (sum 150)
    con.execute(
        "CREATE TABLE h3_8_greenspace_lookup AS SELECT * FROM "
        "(VALUES ('a', 1, 'park', 10.0), ('a', 2, 'wood', 5.0)) t(spatial_id, greenspace_id, function, overlap_area)"
    )
    con.execute(
        "CREATE TABLE h3_8_land_cover_lookup AS SELECT * FROM "
        "(VALUES ('a', 1, 'urban', 7.0)) t(spatial_id, land_cover_id, urban, overlap_area)"
    )
    con.execute(
        "CREATE TABLE h3_8_road_network_lookup AS SELECT * FROM "
        "(VALUES ('a', 1, 'A', 100.0), ('a', 2, 'B', 50.0)) t(spatial_id, road_id, type, overlap_length)"
    )
    con.execute(
        "CREATE TABLE h3_8_retail_centre_lookup AS SELECT 'a' AS spatial_id, 'rc1' AS retail_centre_id, 9.0 AS distance"
    )

    geogs.build(con, [8], True)

    row = con.execute(
        "SELECT greenspace_overlap_area, land_cover_overlap_area, road_overlap_length "
        "FROM h3_8_geogs WHERE spatial_id = 'a'"
    ).fetchone()
    assert tuple(float(v) for v in row or ()) == (10.0, 7.0, 150.0)
