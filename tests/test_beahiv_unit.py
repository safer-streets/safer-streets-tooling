"""Tests for the BEAHIV grid as a spatial unit: its cell geometry, and the steps built over it.

The contract: ``BEAHIV_UNIT`` turns a ``spatial_id`` into the *right* polygon in BNG, and the lookup /
geogs steps produce for it exactly the columns they produce for H3 — which is what makes the two
griddings comparable. The load-bearing claim is that the polygon reaching DuckDB is the one beahiv's
own ``cell_polygon`` returns, unchanged by the WKB round trip; that is asserted directly rather than
by proxy. Synthetic fixtures only — offline-safe, mirroring test_transform_pipeline.
"""

import math

import duckdb
import pytest
from beahiv import cell_polygon
from safer_streets_core.database import duckdb_connector

from safer_streets_tooling.beahiv_grid import CELL_AREA, KEY, SIDE_LENGTH
from safer_streets_tooling.transform import (
    STEPS,
    beahiv,
    beahiv_counts,
    beahiv_geogs,
    beahiv_lookups,
    crime_counts,
    geo_lookups,
    geogs,
    overlap_lookups,
    retail_centre_lookups,
)
from safer_streets_tooling.transform.geo_lookups import GEOGRAPHY_MAPPINGS

_LEEDS = (53.80, -1.50)
_MANCHESTER = (53.40, -2.50)
_CITIES = {"leeds": _LEEDS, "manchester": _MANCHESTER}


def _connect():
    """A writable in-memory connection, or skip if the spatial extensions can't be fetched."""
    try:
        return duckdb_connector(writeable=True)
    except duckdb.HTTPException as e:  # extension download unavailable
        pytest.skip(f"extension download unavailable: {e}")


def _crime_counts(con, beahiv_too=True):
    """crime_data for the two cities, aggregated onto the H3 grid and (optionally) the BEAHIV one."""
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
    crime_counts.build(con, True)
    if beahiv_too:
        beahiv_counts.build(con, True)


def test_cell_geom_matches_beahiv_cell_polygon():
    """The cell reaching DuckDB is vertex-for-vertex the one beahiv's own cell_polygon returns.

    This is the whole BEAHIV unit contract in one assertion: the vectorised UDF decoded each id and
    the WKB round trip preserved the geometry exactly. A wrong CRS, a swapped x/y or a misaligned
    return vector would all show up here as a displaced or misshapen cell.
    """
    con = _connect()
    _crime_counts(con)
    beahiv.register_udfs(con)  # the unit's SQL calls the polygon UDF

    cells = f"({beahiv.BEAHIV_UNIT.cells})"
    spatial_ids = [row[0] for row in con.execute(f"SELECT spatial_id FROM {cells} ORDER BY spatial_id").fetchall()]
    assert spatial_ids

    for spatial_id in spatial_ids:
        expected = cell_polygon(spatial_id).exterior.coords
        ring = con.execute(
            f"SELECT ST_X(pt), ST_Y(pt) FROM ("
            f"  SELECT i, ST_PointN(ST_ExteriorRing(c.cell_geom), i::INTEGER) AS pt"
            f"  FROM {cells} c, generate_series(1, 7) g(i) WHERE c.spatial_id = ?"
            f") ORDER BY i",
            [spatial_id],
        ).fetchall()
        assert len(ring) == 7  # six vertices plus the closing point
        assert ring[-1] == ring[0]
        for (gx, gy), (ex, ey) in zip(ring, expected, strict=True):
            assert gx == pytest.approx(ex, abs=1e-6)
            assert gy == pytest.approx(ey, abs=1e-6)


def test_cell_area_is_the_exact_hexagon_area():
    """cell_area is the analytic 3*sqrt(3)/2*s² — and agrees with the built polygon's own ST_Area.

    The grid is equal-area in EPSG:27700, so this is a constant rather than a per-cell measure; it is
    the *planar* BNG area, matching the planar {prefix}_overlap_area columns it is the denominator for.

    The analytic formula is restated *here* on purpose. ``CELL_AREA`` is measured off beahiv's own
    reference cell rather than computed, so checking it against the SQL polygon alone would compare
    two things derived from the same source; the closed form is the independent third opinion that
    catches a wrong side length or orientation in either of them.
    """
    con = _connect()
    _crime_counts(con)
    beahiv.register_udfs(con)

    analytic = 1.5 * math.sqrt(3.0) * SIDE_LENGTH**2
    declared_constant = CELL_AREA
    assert declared_constant == pytest.approx(analytic, rel=1e-12)

    declared, measured = con.execute(
        f"SELECT {beahiv.BEAHIV_UNIT.area}, ST_Area(cell_geom) FROM ({beahiv.BEAHIV_UNIT.cells}) LIMIT 1"
    ).fetchone()
    assert float(declared) == pytest.approx(analytic, rel=1e-12)
    assert float(measured) == pytest.approx(analytic, rel=1e-9)


def test_relation_names_follow_the_unit_key():
    """Every relation on this grid carries one key, so it needs no naming special case."""
    key, counts_table = KEY, beahiv.COUNTS_TABLE
    assert key == f"beahiv{SIDE_LENGTH}"
    assert beahiv.BEAHIV_UNIT.key == key
    assert counts_table == f"{key}_crime_counts"


def test_steps_are_a_noop_without_the_counts():
    """A build that skipped beahiv_counts leaves nothing to look up, so the steps do nothing and
    `outputs` stays honest about it (mirroring how a hotspot step behaves without its extract)."""
    con = _connect()
    _crime_counts(con, beahiv_too=False)

    assert not beahiv.available(con)
    beahiv_lookups.build(con, True)
    beahiv_geogs.build(con, True)
    assert beahiv_lookups.outputs(con) == []
    assert beahiv_geogs.outputs(con) == []


def test_lookup_and_geogs_steps_build_the_beahiv_relations():
    """The per-cell lookups and geogs exist for the BEAHIV grid, named by its key."""
    con = _connect()
    _crime_counts(con)
    con.execute("""
        CREATE TABLE retail_centres AS SELECT 'rc1' AS rc_id,
            ST_Buffer(ST_Transform(ST_Point(-1.501, 53.801), 'EPSG:4326', 'EPSG:27700', always_xy := true), 50) AS geom
    """)

    beahiv_lookups.build(con, True)
    beahiv_geogs.build(con, True)

    # the geography lookups are built (geogs reads them) but not published: their codes are columns of
    # beahiv202_geogs over the same cells, so a parquet each would be the same data twice
    assert beahiv_lookups.outputs(con) == [f"{KEY}_retail_centre_lookup"]
    for key in GEOGRAPHY_MAPPINGS:
        assert con.execute(f"SELECT COUNT(*) FROM {KEY}_{key}_lookup").fetchone()[0] > 0
    assert beahiv_geogs.outputs(con) == [f"{KEY}_geogs"]
    assert con.execute(f"SELECT COUNT(*) FROM {KEY}_geogs").fetchone()[0] > 0


def test_geogs_schema_matches_h3_apart_from_the_id_type():
    """h3r9_geogs and beahiv202_geogs carry the same columns in the same order, so the two griddings
    are directly comparable and a consumer can swap one for the other.

    Only ``spatial_id`` differs, and only in type: an H3 cell is identified by its canonical hex
    *string*, a BEAHIV cell by its integer id. That is a property of the two indexings rather than of
    these tables — a consumer joining counts to geogs stays within one grid, where the types agree.
    """
    con = _connect()
    _crime_counts(con)

    geo_lookups.build(con, True)
    overlap_lookups.build(con, True)
    retail_centre_lookups.build(con, True)
    geogs.build(con, True)
    beahiv_lookups.build(con, True)
    beahiv_geogs.build(con, True)

    def schema(table):
        return con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = ? ORDER BY ordinal_position",
            [table],
        ).fetchall()

    beahiv_schema, h3_schema = schema(f"{KEY}_geogs"), schema("h3r9_geogs")
    assert [name for name, _ in beahiv_schema] == [name for name, _ in h3_schema]
    assert beahiv_schema[0] == ("spatial_id", "BIGINT")
    assert h3_schema[0] == ("spatial_id", "VARCHAR")
    assert beahiv_schema[1:] == h3_schema[1:]


def test_cells_land_in_the_right_geography():
    """Each BEAHIV cell resolves to the ONS code of the city its crimes came from.

    The cells come from beahiv in BNG with no reprojection; if that were wrong (e.g. treating the
    coordinates as lat/lon) they would fall outside every boundary and the codes would be NULL.
    """
    con = _connect()
    _crime_counts(con)
    beahiv_lookups.build(con, True)

    per_cell = dict(
        con.execute(f"""
            SELECT c.spatial_id, l.lad24cd
            FROM {beahiv.COUNTS_TABLE} c
            JOIN {KEY}_lad24cd_lookup l USING (spatial_id)
        """).fetchall()
    )
    assert sorted(per_cell.values()) == ["leeds", "manchester"]


def test_steps_registered_in_dependency_order():
    """beahiv_counts → beahiv_lookups → beahiv_geogs, the H3 chain's shape on the other grid."""
    names = [step.name for step in STEPS]
    assert names.index("beahiv_counts") < names.index("beahiv_lookups") < names.index("beahiv_geogs")
    assert beahiv_lookups.STEP.depends_on == ("beahiv_counts",)
    # geogs also lists beahiv_counts directly: the geography lookups between them publish no parquet,
    # so they carry no mtime for the staleness check
    assert beahiv_geogs.STEP.depends_on == ("beahiv_counts", "beahiv_lookups")


def test_extract_cell_id_columns_tag_the_cell_containing_the_feature():
    """The ids the extracts tag onto each feature are the cells that actually contain it.

    This is what makes the two phases joinable: the extract writes ``beahiv202_id`` / ``h3r9_id``
    columns, the transform aggregates crimes onto grids of the same name, and a consumer joins one to
    the other. Two encodings of "the same" grid that disagreed would join to nothing without ever
    raising, so the tagged cell is checked against beahiv's own polygon rather than against the encoder
    that produced it.
    """
    from shapely import Point

    from safer_streets_tooling.extract._common import cell_id_columns
    from safer_streets_tooling.grids import BEAHIV_ID, H3_ID

    con = _connect()
    con.execute(f"""
        CREATE TABLE features AS
        SELECT city, lat, lon,
               ST_Transform(ST_Point(lon, lat), 'EPSG:4326', 'EPSG:27700', always_xy := true) AS geom
        FROM (VALUES {", ".join(f"('{c}', {lat}, {lon})" for c, (lat, lon) in _CITIES.items())}) t(city, lat, lon)
    """)
    rows = con.execute(f"""
        SELECT city, ST_X(geom), ST_Y(geom), {cell_id_columns(con, "lat", "lon", "geom")}
        FROM features ORDER BY city
    """).fetchall()
    assert [r[0] for r in rows] == sorted(_CITIES)

    columns = [d[0] for d in con.description]
    assert columns[-2:] == [H3_ID, BEAHIV_ID]  # named off the grid keys, not spelled out here

    for _city, x, y, h3_id, beahiv_id in rows:
        assert isinstance(h3_id, str) and len(h3_id) == 15  # a res-9 cell's canonical hex
        assert cell_polygon(beahiv_id).contains(Point(x, y))  # beahiv's own geometry, not the encoder's
