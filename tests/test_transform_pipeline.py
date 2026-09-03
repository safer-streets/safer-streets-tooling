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

    # the hotspot hexes are their own grid (cells come from the extract, not from crime_counts), so
    # only hotspot_geogs waits on anything
    assert pipeline.nodes["hotspot_counts"].dependency_ids == ()
    assert pipeline.nodes["hotspot_lookups"].dependency_ids == ()
    assert pipeline.nodes["hotspot_geogs"].dependency_ids == ("hotspot_lookups",)


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


# the distinct crime locations in _crime_data, reused to build the boundary fixtures around them
_CITIES = {"leeds": (53.80, -1.50), "manchester": (53.40, -2.50), "london": (51.50, -0.12)}


def _crime_data(con):
    """Six crime_data rows: four countable, one BTP and one un-geolocated (both excluded). Carries the
    BNG point ``geom`` the extractor adds (the geography counts join on it)."""
    con.execute("""
        CREATE TABLE crime_data AS SELECT *,
            CASE WHEN latitude IS NULL THEN NULL
                 ELSE ST_Transform(ST_Point(longitude, latitude), 'EPSG:4326', 'EPSG:27700', always_xy := true)
            END AS geom
        FROM (VALUES
            (53.80, -1.50, 'Burglary',      '2024-01', 'West Yorkshire Police'),
            (53.80, -1.50, 'Burglary',      '2024-01', 'West Yorkshire Police'),
            (53.40, -2.50, 'Bicycle theft', '2024-02', 'Greater Manchester Police'),
            (51.50, -0.12, 'Other theft',   '2024-01', 'Metropolitan Police Service'),
            (53.80, -1.50, 'Robbery',       '2024-01', 'British Transport Police'),
            (NULL,  NULL,  'Public order',  '2024-03', 'West Yorkshire Police')
        ) t(latitude, longitude, crime_type, _month, falls_within)
    """)


def _boundary_tables(con, cities=tuple(_CITIES)):
    """Every ONS boundary table gets one 1 km polygon per city, keyed by the city name, so each
    geolocated crime falls in exactly one polygon (omit a city to leave its crimes uncovered)."""
    values = ", ".join(f"('{city}', {lat}, {lon})" for city, (lat, lon) in _CITIES.items() if city in cities)
    for table in GEOGRAPHY_MAPPINGS.values():
        con.execute(f"""
            CREATE OR REPLACE TABLE "{table}" AS
            SELECT city AS spatial_id,
                ST_Buffer(ST_Transform(ST_Point(lon, lat), 'EPSG:4326', 'EPSG:27700', always_xy := true), 1000) AS geom
            FROM (VALUES {values}) t(city, lat, lon)
        """)


def _hotspot_table(con, cities=("leeds",), radius=1000):
    """A hotspots table with one polygon per named city, standing in for the 350m hex grid: a partial
    grid (only some cities) is the realistic case — the hexes cover only the flagged parts of the map."""
    values = ", ".join(f"('{city}', {lat}, {lon})" for city, (lat, lon) in _CITIES.items() if city in cities)
    con.execute(f"""
        CREATE OR REPLACE TABLE hotspots AS
        SELECT
            city AS spatial_id,
            'Test Constabulary' AS pfa,
            'VRSK' AS hits,
            ST_Buffer(ST_Transform(ST_Point(lon, lat), 'EPSG:4326', 'EPSG:27700', always_xy := true), {radius}) AS geom
        FROM (VALUES {values}) t(city, lat, lon)
    """)


def test_crime_counts_conserves_filtered_input():
    """The per-cell counts sum back to the geolocated, non-BTP input rows at every resolution."""
    from safer_streets_tooling.transform import crime_counts

    con = _connect()  # needs the h3 extension (h3_latlng_to_cell)
    _crime_data(con)
    _boundary_tables(con)

    crime_counts.build(con, [8, 9, 10], True)

    for res in (8, 9, 10):
        total = con.execute(f"SELECT SUM(count) FROM crime_counts_h3_{res}").fetchone()[0]
        assert total == 4  # 6 rows − 1 BTP − 1 un-geolocated
    assert crime_counts.outputs(con, [8, 9, 10]) == [f"crime_counts_h3_{r}" for r in (8, 9, 10)] + [
        f"crime_counts_{key}" for key in GEOGRAPHY_MAPPINGS
    ]


def test_crime_counts_counts_per_ons_geography():
    """Each ONS geography table counts the filtered crimes point-in-polygon, keyed by boundary code /
    crime type / month — same schema and exclusions as the H3 counts."""
    from safer_streets_tooling.transform import crime_counts

    con = _connect()
    _crime_data(con)
    _boundary_tables(con)

    crime_counts.build(con, [9], True)

    for key in GEOGRAPHY_MAPPINGS:
        per_area = dict(
            con.execute(f"SELECT spatial_id, SUM(count) FROM crime_counts_{key} GROUP BY spatial_id").fetchall()
        )
        assert per_area == {"leeds": 2, "manchester": 1, "london": 1}  # BTP + un-geolocated excluded
    row = con.execute(
        "SELECT count FROM crime_counts_pfa23cd "
        "WHERE spatial_id = 'leeds' AND crime_type = 'Burglary' AND month = '2024-01'"
    ).fetchone()
    assert row == (2,)


def test_crime_counts_drops_crimes_outside_boundary_coverage():
    """A crime outside every boundary polygon (e.g. NI crimes vs the E&W-only layers) is dropped from
    the geography counts without raising — only over-counting is an error."""
    from safer_streets_tooling.transform import crime_counts

    con = _connect()
    _crime_data(con)
    _boundary_tables(con, cities=("leeds", "manchester"))  # london crime left uncovered

    crime_counts.build(con, [9], True)

    for key in GEOGRAPHY_MAPPINGS:
        total = con.execute(f"SELECT SUM(count) FROM crime_counts_{key}").fetchone()[0]
        assert total == 3  # the london crime falls in no polygon


def test_crime_counts_raises_when_boundaries_overlap():
    """Overlapping boundary polygons would count a crime in more than one area — the upper-bound
    conservation check raises rather than emitting inflated counts."""
    from safer_streets_tooling.transform import crime_counts

    con = _connect()
    _crime_data(con)
    _boundary_tables(con)
    lat, lon = _CITIES["leeds"]
    con.execute(f"""
        INSERT INTO police_force_areas
        SELECT 'leeds_overlap',
            ST_Buffer(ST_Transform(ST_Point({lon}, {lat}), 'EPSG:4326', 'EPSG:27700', always_xy := true), 1000)
    """)

    with pytest.raises(ValueError, match="more than one area"):
        crime_counts.build(con, [9], True)


class _DropBurglaryFromCounts:
    """A connection proxy that silently drops Burglary rows from the count query only, so the emitted
    table no longer sums to the (unchanged) filtered input — simulating a lossy aggregation."""

    def __init__(self, con):
        self._con = con

    def execute(self, sql, *args, **kwargs):
        if "GROUP BY" in sql:  # only the per-cell count query aggregates
            sql = sql.replace("GROUP BY", "AND crime_type != 'Burglary' GROUP BY")
        return self._con.execute(sql, *args, **kwargs)


def test_crime_counts_raises_when_counts_not_conserved():
    """If the aggregation silently drops crimes, the conservation check raises rather than emitting a skewed grid."""
    from safer_streets_tooling.transform import crime_counts

    con = _connect()
    _crime_data(con)
    _boundary_tables(con)

    with pytest.raises(ValueError, match="not conserved"):
        crime_counts.build(_DropBurglaryFromCounts(con), [9], True)  # ty:ignore[invalid-argument-type]


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


def test_road_intersection_counts_per_cell_restricted_to_crime_grid():
    """Intersection points are placed by their H3 cell at each resolution and counted, keeping only
    cells present in crime_counts (so the count grid lines up with the crime / road-length grid)."""
    from safer_streets_tooling.transform import road_intersection_counts

    con = _connect()  # needs the spatial + h3 extensions (ST_Transform, h3_latlng_to_cell)
    # three intersections (BNG geom): two share one WGS-84 point, one elsewhere
    con.execute("""
        CREATE TABLE road_intersections AS SELECT
            ST_Transform(pt, 'EPSG:4326', 'EPSG:27700', always_xy := true) AS geom
        FROM (VALUES (ST_Point(-1.5, 53.8)), (ST_Point(-1.5, 53.8)), (ST_Point(-2.5, 53.4))) t(pt)
    """)
    cell_a = con.execute("SELECT lower(hex(h3_latlng_to_cell(53.8, -1.5, 9)))").fetchone()[0]

    # only cell_a is in the crime grid → the third point's cell is excluded
    con.execute(f"CREATE TABLE crime_counts_h3_9 AS SELECT '{cell_a}' AS spatial_id")

    road_intersection_counts.build(con, [9], True)

    rows = dict(con.execute("SELECT spatial_id, road_intersection_count FROM road_intersection_counts_h3_9").fetchall())
    assert rows == {cell_a: 2}
    assert road_intersection_counts.outputs(con, [9]) == ["road_intersection_counts_h3_9"]


def test_road_intersection_counts_noop_without_road_intersections_table():
    """The step is a no-op (no table, no output) when the road_intersections extract is absent."""
    from safer_streets_tooling.transform import road_intersection_counts

    con = duckdb.connect()
    road_intersection_counts.build(con, [9], True)  # no road_intersections table → must not raise
    assert road_intersection_counts.outputs(con, [9]) == []


def test_crime_counts_hotspots_counts_per_hex():
    """The hotspot hexes are counted like any other non-overlapping polygon layer: same
    spatial_id / crime_type / month / count schema, same BTP + un-geolocated exclusions. Crimes outside
    the (partial) hotspot grid simply don't appear."""
    from safer_streets_tooling.transform import crime_counts

    con = _connect()
    _crime_data(con)
    _hotspot_table(con, cities=("leeds", "manchester"))  # london is not a hotspot

    crime_counts.build_hotspots(con, True)

    per_hex = dict(
        con.execute("SELECT spatial_id, SUM(count) FROM crime_counts_hotspots GROUP BY spatial_id").fetchall()
    )
    assert per_hex == {"leeds": 2, "manchester": 1}  # BTP, un-geolocated and the london crime excluded
    assert crime_counts.hotspot_outputs(con) == ["crime_counts_hotspots"]


def test_crime_counts_hotspots_raises_when_hexes_overlap():
    """Overlapping hexes would count a crime twice — the same upper-bound check as the ONS geographies
    raises rather than emitting inflated counts. (Like that check it is an upper bound on the *whole*
    input, so it only catches double-counting once the grid covers enough of it.)"""
    from safer_streets_tooling.transform import crime_counts

    con = _connect()
    _crime_data(con)
    _hotspot_table(con, cities=tuple(_CITIES))
    lat, lon = _CITIES["leeds"]
    con.execute(f"""
        INSERT INTO hotspots
        SELECT 'leeds_overlap', 'Test Constabulary', 'V',
            ST_Buffer(ST_Transform(ST_Point({lon}, {lat}), 'EPSG:4326', 'EPSG:27700', always_xy := true), 1000)
    """)

    with pytest.raises(ValueError, match="more than one area"):
        crime_counts.build_hotspots(con, True)


def test_point_layer_counts_per_hex():
    """Street lights and road intersections are placed in a hex by their BNG point (no H3 detour), and
    buildings by their footprint centroid — the point their h3_9_id also comes from."""
    from safer_streets_tooling.transform import building_counts, road_intersection_counts, streetlight_counts

    con = _connect()
    _hotspot_table(con, cities=("leeds", "manchester"))
    leeds, london = _CITIES["leeds"], _CITIES["london"]
    con.execute(f"""
        CREATE TABLE streetlights AS SELECT
            ST_Transform(pt, 'EPSG:4326', 'EPSG:27700', always_xy := true) AS geom, 'x' AS h3_9_id
        FROM (VALUES (ST_Point({leeds[1]}, {leeds[0]})), (ST_Point({leeds[1]}, {leeds[0]})),
                     (ST_Point({london[1]}, {london[0]}))) t(pt)
    """)
    con.execute("CREATE TABLE road_intersections AS SELECT geom FROM streetlights")
    # footprints: a 50m buffer round each point, so the centroid is what decides the hex
    con.execute("""
        CREATE TABLE buildings AS
        SELECT ST_Buffer(geom, 50) AS geom, 'Residential' AS map_simple_use, 'x' AS h3_9_id FROM streetlights
    """)

    streetlight_counts.build_hotspots(con, True)
    building_counts.build_hotspots(con, True)
    road_intersection_counts.build_hotspots(con, True)

    assert dict(con.execute("SELECT spatial_id, streetlight_count FROM streetlight_counts_hotspots").fetchall()) == {
        "leeds": 2
    }  # the london light is outside the grid
    assert dict(
        con.execute("SELECT spatial_id, road_intersection_count FROM road_intersection_counts_hotspots").fetchall()
    ) == {"leeds": 2}
    assert con.execute(
        "SELECT spatial_id, map_simple_use, building_count FROM building_counts_hotspots"
    ).fetchall() == [("leeds", "Residential", 2)]


def _hotspot_population_inputs(con):
    """One OA whose three residential buildings straddle the grid: two inside the leeds hex, one not."""
    leeds, london = _CITIES["leeds"], _CITIES["london"]
    con.execute(f"""
        CREATE TABLE buildings AS SELECT
            oa21cd, map_simple_use, premise_area, gross_area, 'x' AS h3_9_id,
            ST_Transform(pt, 'EPSG:4326', 'EPSG:27700', always_xy := true) AS geom
        FROM (VALUES
            ('OA1', 'Residential', 100.0, 100.0, ST_Point({leeds[1]}, {leeds[0]})),
            ('OA1', 'Residential', 100.0, 100.0, ST_Point({leeds[1]}, {leeds[0]})),
            ('OA1', 'Residential', 100.0, 100.0, ST_Point({london[1]}, {london[0]}))
        ) t(oa21cd, map_simple_use, premise_area, gross_area, pt)
    """)
    con.execute("CREATE TABLE workplace_population AS SELECT 'OA1' AS spatial_id, 60 AS workplace_population")
    con.execute(
        "CREATE TABLE residential_population AS "
        "SELECT 'OA1' AS spatial_id, 300 AS household_population, 0 AS communal_population"
    )


def test_population_counts_hotspots_allocates_only_the_hexes_share():
    """The OA shares are worked out over *every* building, then the ones outside the grid are dropped —
    so a hex holding two of an OA's three equal buildings gets two thirds of its population, not all of
    it (which is what renormalising within the grid would give)."""
    from safer_streets_tooling.transform import population_counts

    con = _connect()
    _hotspot_table(con)  # leeds only
    _hotspot_population_inputs(con)

    population_counts.build_hotspots(con, True)

    rows = con.execute(
        "SELECT spatial_id, residential_population, workplace_population FROM population_counts_hotspots"
    ).fetchall()
    assert len(rows) == 1
    spatial_id, residential, workplace = rows[0]
    assert spatial_id == "leeds"
    assert residential == pytest.approx(200.0)  # 2 of the OA's 3 equal buildings, not the whole 300
    assert workplace == pytest.approx(0.0)  # no Non Residential / Mixed Use building to take it
    assert population_counts.hotspot_outputs(con) == ["population_counts_hotspots"]


def test_hotspot_lookups_and_geogs_describe_each_hex():
    """The hotspot lookups + geogs are the H3 ones on a different set of cells: each hex maps to the ONS
    code it overlaps most, and hotspots_geogs carries that code plus the hex's own polygon area."""
    from safer_streets_tooling.transform import hotspot_geogs, hotspot_lookups

    con = _connect()
    _boundary_tables(con)
    _hotspot_table(con, cities=("leeds", "manchester"))

    hotspot_lookups.build(con, [9], True)
    hotspot_geogs.build(con, [9], True)

    assert dict(con.execute("SELECT spatial_id, lad24cd FROM hotspots_lad24cd_lookup").fetchall()) == {
        "leeds": "leeds",
        "manchester": "manchester",
    }
    rows = dict(con.execute("SELECT spatial_id, cell_area FROM hotspots_geogs").fetchall())
    assert rows.keys() == {"leeds", "manchester"}
    for area in rows.values():
        assert float(area) == pytest.approx(3.14e6, rel=0.01)  # the 1km-radius fixture polygon, in m²
    assert set(hotspot_lookups.outputs(con, [9])) >= {f"hotspots_{key}_lookup" for key in GEOGRAPHY_MAPPINGS}
    assert hotspot_geogs.outputs(con, [9]) == ["hotspots_geogs"]


def test_hotspot_steps_are_noops_without_the_hotspots_table():
    """Every hotspot step is a clean no-op (no relation, no output) when the optional extract is absent."""
    from safer_streets_tooling.transform import hotspot_counts, hotspot_geogs, hotspot_lookups

    con = _connect()
    _crime_data(con)  # a source layer is present; only the hexes are missing

    for step in (hotspot_counts, hotspot_lookups, hotspot_geogs):
        step.build(con, [9], True)  # must not raise
        assert step.outputs(con, [9]) == []
