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
    assert pipeline.nodes["streetlight_counts"].dependency_ids == ()  # independent of crime_counts
    assert pipeline.nodes["population_counts"].dependency_ids == ()  # independent of crime_counts
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


def test_geogs_includes_cell_area_and_folds_overlaps():
    """h3_geogs carries the H3 cell area (m², via h3_cell_area) plus, per area layer, the largest overlap
    (summing would double-count overlapping polygons) and the total road length. Uses a real H3 cell so
    h3_cell_area resolves (needs the spatial/h3 extensions)."""
    con = _connect()
    cell = con.execute("SELECT lower(hex(h3_latlng_to_cell(51.5, -0.1, 8)))").fetchone()[0]

    # source-table presence drives which overlap features / retail centres are folded in
    for table in ("open_greenspace", "land_cover", "open_roads", "retail_centres"):
        con.execute(f"CREATE TABLE {table}(x INTEGER)")
    # one row per ONS geography lookup for the cell
    for key in GEOGRAPHY_MAPPINGS:
        con.execute(f"CREATE TABLE h3_8_{key}_lookup AS SELECT '{cell}' AS spatial_id, 'X' AS {key}")
    # two greenspace polygons (largest 10), urban land-cover overlap 7, suburban 3, two road segments (sum 150)
    con.execute(
        f"CREATE TABLE h3_8_greenspace_lookup AS SELECT * FROM "
        f"(VALUES ('{cell}', 1, 'park', 10.0), ('{cell}', 2, 'wood', 5.0)) "
        f"t(spatial_id, greenspace_id, function, overlap_area)"
    )
    con.execute(
        f"CREATE TABLE h3_8_urban_lookup AS SELECT * FROM "
        f"(VALUES ('{cell}', 1, 7.0)) t(spatial_id, urban_id, overlap_area)"
    )
    con.execute(
        f"CREATE TABLE h3_8_suburban_lookup AS SELECT * FROM "
        f"(VALUES ('{cell}', 2, 3.0)) t(spatial_id, suburban_id, overlap_area)"
    )
    con.execute(
        f"CREATE TABLE h3_8_road_network_lookup AS SELECT * FROM "
        f"(VALUES ('{cell}', 1, 'A', 100.0), ('{cell}', 2, 'B', 50.0)) t(spatial_id, road_id, type, overlap_length)"
    )
    con.execute(
        f"CREATE TABLE h3_8_retail_centre_lookup AS "
        f"SELECT '{cell}' AS spatial_id, 'rc1' AS retail_centre_id, 9.0 AS distance"
    )

    geogs.build(con, [8], True)

    cell_area, gs, ur, sub, rl = con.execute(
        f"SELECT cell_area, greenspace_overlap_area, urban_overlap_area, suburban_overlap_area, road_overlap_length "
        f"FROM h3_8_geogs WHERE spatial_id = '{cell}'"
    ).fetchone()
    assert 100_000 < float(cell_area) < 2_000_000  # a res-8 H3 cell is ~0.66 km² = ~660,000 m²
    assert (float(gs), float(ur), float(sub), float(rl)) == (10.0, 7.0, 3.0, 150.0)


def test_streetlight_counts_aggregates_per_res9_cell():
    """streetlight_counts groups the streetlights extract into a per-res-9-cell count keyed by spatial_id."""
    from safer_streets_tooling.transform import streetlight_counts

    con = duckdb.connect()  # plain group-by, no spatial/h3 extensions needed
    con.execute("CREATE TABLE streetlights AS SELECT * FROM (VALUES ('aaa'), ('aaa'), ('aaa'), ('bbb')) t(h3_9_id)")

    streetlight_counts.build(con, [9], True)

    rows = dict(con.execute("SELECT spatial_id, streetlight_count FROM streetlight_counts_h3_9").fetchall())
    assert rows == {"aaa": 3, "bbb": 1}
    assert streetlight_counts.outputs(con, [9]) == ["streetlight_counts_h3_9"]


def _population_inputs(con):
    """Two-OA fixture: buildings spanning three cells plus both per-OA population extracts."""
    con.execute("""
        CREATE TABLE buildings AS SELECT * FROM (VALUES
            (1, 'OA1', 'aaa', 'Non Residential', 100.0, 300.0),
            (2, 'OA1', 'aaa', 'Mixed Use',       100.0, 200.0),
            (3, 'OA1', 'bbb', 'Residential',     100.0, 200.0),
            (4, 'OA1', 'bbb', 'Non Residential', 100.0, NULL),
            (5, NULL,  'ccc', 'Non Residential', 100.0, 100.0),
            (6, 'OA2', 'ccc', 'Residential',     100.0, 400.0)
        ) t(verisk_premise_id, oa21cd, h3_9_id, map_simple_use, premise_area, gross_area)
    """)
    con.execute(
        "CREATE TABLE workplace_population AS SELECT * FROM "
        "(VALUES ('OA1', 500), ('OA2', 50)) t(spatial_id, workplace_population)"
    )
    con.execute(
        "CREATE TABLE residential_population AS SELECT * FROM "
        "(VALUES ('OA1', 270, 30), ('OA2', 10, 0)) t(spatial_id, household_population, communal_population)"
    )


def test_population_counts_assigns_by_use_then_sums_per_cell():
    """Each OA's populations are assigned to buildings pro rata to floor area × use weight (workplace →
    Non Residential ×1 / Mixed ×0.5; residential → Residential ×1 / Mixed ×0.5), then summed per res-9
    cell. Residential buildings take no workplace population and vice versa; OA-less buildings nothing."""
    from safer_streets_tooling.transform import population_counts

    con = duckdb.connect()  # plain SQL, no spatial/h3 extensions needed
    _population_inputs(con)

    population_counts.build(con, [9], True)

    rows = {
        r[0]: (r[1], r[2])
        for r in con.execute(
            "SELECT spatial_id, residential_population, workplace_population FROM population_counts_h3_9"
        ).fetchall()
    }
    # OA1 workplace 500 over work weights #1 300, #2 200×0.5=100, #4 premise-fallback 100 → 300/100/100
    # OA1 residential 270+30=300 over res weights #2 100, #3 200 → 100/200
    # OA2 workplace 50 has no eligible building → unassigned; OA2 residential 10 → #6; #5 has no OA
    assert rows.keys() == {"aaa", "bbb", "ccc"}
    assert rows["aaa"] == (pytest.approx(100.0), pytest.approx(400.0))  # res: #2; work: #1 + #2
    assert rows["bbb"] == (pytest.approx(200.0), pytest.approx(100.0))  # res: #3; work: #4
    assert rows["ccc"] == (pytest.approx(10.0), pytest.approx(0.0))  # res: #6; work: none assignable

    # consistency: everything assignable is conserved onto the grid
    res_total, work_total = con.execute(
        "SELECT SUM(residential_population), SUM(workplace_population) FROM population_counts_h3_9"
    ).fetchone()  # ty:ignore[not-iterable]
    assert res_total == pytest.approx(310.0)  # all 300 (OA1) + 10 (OA2) residents land
    assert work_total == pytest.approx(500.0)  # OA1's 500 land; OA2's 50 have nowhere to go

    assert population_counts.outputs(con, [9]) == ["population_counts_h3_9"]


def test_population_counts_noop_without_input_tables():
    """The step is a no-op (no table, no output) unless all three input extracts are present."""
    from safer_streets_tooling.transform import population_counts

    con = duckdb.connect()
    population_counts.build(con, [9], True)  # no tables → must not raise
    assert population_counts.outputs(con, [9]) == []

    con.execute("CREATE TABLE buildings(verisk_premise_id INTEGER)")
    con.execute("CREATE TABLE workplace_population(spatial_id VARCHAR)")
    population_counts.build(con, [9], True)  # residential_population absent → still a no-op
    assert population_counts.outputs(con, [9]) == []


def test_population_counts_noop_when_buildings_predate_size_columns():
    """A cached buildings parquet from before the size columns were added is skipped, not crashed on."""
    from safer_streets_tooling.transform import population_counts

    con = duckdb.connect()
    con.execute("CREATE TABLE buildings AS SELECT 1 AS verisk_premise_id, 'OA1' AS oa21cd")  # no area columns
    con.execute("CREATE TABLE workplace_population AS SELECT 'OA1' AS spatial_id, 500 AS workplace_population")
    con.execute(
        "CREATE TABLE residential_population AS "
        "SELECT 'OA1' AS spatial_id, 10 AS household_population, 0 AS communal_population"
    )

    population_counts.build(con, [9], True)  # must not raise

    assert population_counts.outputs(con, [9]) == []


def test_streetlight_counts_noop_without_streetlights_table():
    """The step is a no-op (no table, no output) when the streetlights extract is absent."""
    from safer_streets_tooling.transform import streetlight_counts

    con = duckdb.connect()
    streetlight_counts.build(con, [9], True)  # no streetlights table → must not raise
    assert streetlight_counts.outputs(con, [9]) == []
