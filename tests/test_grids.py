"""Tests for the grid abstraction the per-cell lookups are built over.

The contract: a ``Grid`` turns a ``spatial_id`` into the *right* polygon in BNG, and the lookups /
``geogs`` steps produce the same columns for every grid. The load-bearing claim for BEAHIV is that
the hexagon SQL builds — from a UDF centre plus constant vertex offsets — is the same polygon
beahiv's own ``cell_polygon`` returns; that is asserted directly rather than by proxy.
Synthetic fixtures only — offline-safe, mirroring test_transform_pipeline.
"""

import math

import duckdb
import pytest
from beahiv import cell_polygon
from safer_streets_core.database import duckdb_connector

from safer_streets_tooling.transform import beahiv_counts, geo_lookups, geogs, overlap_lookups, retail_centre_lookups
from safer_streets_tooling.transform.beahiv_counts import cell_ids
from safer_streets_tooling.transform.geo_lookups import GEOGRAPHY_MAPPINGS
from safer_streets_tooling.transform.grids import BEAHIV_GRID, SIDE_LENGTH, grids, h3_grid

_LEEDS = (53.80, -1.50)
_MANCHESTER = (53.40, -2.50)
_CITIES = {"leeds": _LEEDS, "manchester": _MANCHESTER}


def _connect():
    """A writable in-memory connection, or skip if the spatial extensions can't be fetched."""
    try:
        return duckdb_connector(writeable=True)
    except duckdb.HTTPException as e:  # extension download unavailable
        pytest.skip(f"extension download unavailable: {e}")


def _crime_counts(con):
    """crime_data for the two cities, aggregated onto both grids (so both have cells to look up)."""
    from safer_streets_tooling.transform import crime_counts

    values = ", ".join(f"({lat}, {lon}, 'Burglary', '2024-01', 'Police')" for lat, lon in _CITIES.values())
    con.execute(f"""
        CREATE OR REPLACE TABLE crime_data AS SELECT *,
            ST_Transform(ST_Point(longitude, latitude), 'EPSG:4326', 'EPSG:27700', always_xy := true) AS geom
        FROM (VALUES {values}) t(latitude, longitude, crime_type, _month, falls_within)
    """)
    for table in GEOGRAPHY_MAPPINGS.values():
        con.execute(f"""
            CREATE OR REPLACE TABLE "{table}" AS
            SELECT city AS spatial_id,
                ST_Buffer(ST_Transform(ST_Point(lon, lat), 'EPSG:4326', 'EPSG:27700', always_xy := true), 1000) AS geom
            FROM (VALUES {", ".join(f"('{c}', {lat}, {lon})" for c, (lat, lon) in _CITIES.items())}) t(city, lat, lon)
        """)
    crime_counts.build(con, [9], True)
    beahiv_counts.build(con, [9], True)


def test_beahiv_cell_geom_matches_beahiv_cell_polygon():
    """The hexagon the SQL builds is vertex-for-vertex the one beahiv's own cell_polygon returns.

    This is the whole BEAHIV grid contract in one assertion: the vectorised centre UDF decoded the id
    correctly *and* the constant vertex offsets are the right ones. A wrong CRS, a swapped x/y or a
    stale offset table would all show up here as a displaced or misshapen cell.
    """
    con = _connect()
    _crime_counts(con)
    grids(con, [9])  # registers the centre UDF the grid's SQL calls

    cells = [
        row[0] for row in con.execute(f"SELECT spatial_id FROM {BEAHIV_GRID.cells} ORDER BY spatial_id").fetchall()
    ]
    assert cells

    for spatial_id in cells:
        expected = cell_polygon(int(cell_ids([spatial_id])[0]))
        ring = con.execute(
            f"SELECT ST_X(pt), ST_Y(pt) FROM ("
            f"  SELECT i, ST_PointN(ST_ExteriorRing(c.cell_geom), i::INTEGER) AS pt"
            f"  FROM {BEAHIV_GRID.cells} c, generate_series(1, 7) g(i) WHERE c.spatial_id = ?"
            f") ORDER BY i",
            [spatial_id],
        ).fetchall()
        assert len(ring) == 7  # six vertices plus the closing point
        assert ring[-1] == ring[0]
        for (gx, gy), (ex, ey) in zip(ring[:6], expected, strict=True):
            assert gx == pytest.approx(ex, abs=1e-6)
            assert gy == pytest.approx(ey, abs=1e-6)


def test_beahiv_cell_area_is_the_exact_hexagon_area():
    """cell_area is the analytic 3*sqrt(3)/2*s² — and agrees with the built polygon's own ST_Area.

    The grid is equal-area in EPSG:27700, so this is a constant rather than a per-cell measure; it is
    the *planar* BNG area, matching the planar {prefix}_overlap_area columns it is the denominator for.
    """
    con = _connect()
    _crime_counts(con)
    grids(con, [9])

    analytic = 1.5 * math.sqrt(3.0) * SIDE_LENGTH**2
    declared, measured = con.execute(
        f"SELECT {BEAHIV_GRID.cell_area.replace('base.spatial_id', 'spatial_id')}, ST_Area(cell_geom) "
        f"FROM {BEAHIV_GRID.cells} LIMIT 1"
    ).fetchone()
    assert float(declared) == pytest.approx(analytic, rel=1e-12)
    assert float(measured) == pytest.approx(analytic, rel=1e-9)


def test_grid_counts_table_follows_one_rule():
    """Every grid reads crime_counts_{key}, so a new grid needs no naming special case."""
    assert h3_grid(9).counts_table == "crime_counts_h3_9"
    assert BEAHIV_GRID.counts_table == f"crime_counts_beahiv_{SIDE_LENGTH}"
    assert BEAHIV_GRID.key == f"beahiv_{SIDE_LENGTH}"


def test_beahiv_grid_skipped_when_its_counts_are_absent():
    """A build without the beahiv step present yields the H3 grids only — so `outputs` stays honest
    about what `build` will create (mirroring how an absent overlap source skips its feature)."""
    con = _connect()

    assert [g.key for g in grids(con, [8, 9])] == ["h3_8", "h3_9"]

    _crime_counts(con)
    assert [g.key for g in grids(con, [8, 9])] == ["h3_8", "h3_9", f"beahiv_{SIDE_LENGTH}"]


def test_lookup_steps_build_every_grid():
    """The per-cell lookups and geogs are built for the BEAHIV grid as well as each H3 resolution."""
    con = _connect()
    _crime_counts(con)
    con.execute("""
        CREATE TABLE retail_centres AS SELECT 'rc1' AS rc_id,
            ST_Buffer(ST_Transform(ST_Point(-1.501, 53.801), 'EPSG:4326', 'EPSG:27700', always_xy := true), 50) AS geom
    """)

    geo_lookups.build(con, [9], True)
    overlap_lookups.build(con, [9], True)
    retail_centre_lookups.build(con, [9], True)
    geogs.build(con, [9], True)

    beahiv = f"beahiv_{SIDE_LENGTH}"
    assert geo_lookups.outputs(con, [9]) == [
        f"{g}_{key}_lookup" for g in ("h3_9", beahiv) for key in GEOGRAPHY_MAPPINGS
    ]
    assert retail_centre_lookups.outputs(con, [9]) == ["h3_9_retail_centre_lookup", f"{beahiv}_retail_centre_lookup"]
    assert geogs.outputs(con, [9]) == ["h3_9_geogs", f"{beahiv}_geogs"]


def test_geogs_schema_is_identical_across_grids():
    """h3_9_geogs and beahiv_202_geogs carry the same columns and types, so the two griddings are
    directly comparable and a consumer can swap one for the other."""
    con = _connect()
    _crime_counts(con)

    geo_lookups.build(con, [9], True)
    overlap_lookups.build(con, [9], True)
    retail_centre_lookups.build(con, [9], True)
    geogs.build(con, [9], True)

    def schema(table):
        return con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = ? ORDER BY ordinal_position",
            [table],
        ).fetchall()

    assert schema(f"beahiv_{SIDE_LENGTH}_geogs") == schema("h3_9_geogs")


def test_beahiv_cells_land_in_the_right_geography():
    """Each BEAHIV cell resolves to the ONS code of the city its crimes came from.

    The cells are built in BNG with no reprojection; if that were wrong (e.g. treating the centres as
    lat/lon) the cells would fall outside every boundary and the codes would come back NULL.
    """
    con = _connect()
    _crime_counts(con)
    geo_lookups.build(con, [9], True)

    per_cell = dict(
        con.execute(f"""
            SELECT c.spatial_id, l.lad24cd
            FROM {beahiv_counts.table_name()} c
            JOIN beahiv_{SIDE_LENGTH}_lad24cd_lookup l USING (spatial_id)
        """).fetchall()
    )
    assert sorted(per_cell.values()) == ["leeds", "manchester"]
