"""Tests for the BEAHIV crime-count transform step.

The contract: the same crimes as the H3 grids (same filter, same conservation guarantee), keyed by a
BEAHIV cell id that agrees with beahiv's own scalar encoder and decodes back to the declared grid.
Synthetic fixtures only — offline-safe, mirroring test_transform_pipeline.
"""

import duckdb
import pytest
from beahiv import Orientation, bng_to_cell, decode, latlon_to_cell
from duckdb.sqltypes import DOUBLE, UBIGINT
from safer_streets_core.database import duckdb_connector

from safer_streets_tooling.transform import beahiv_counts
from safer_streets_tooling.transform.beahiv_counts import ORIENTATION, SIDE_LENGTH, cell_ids, table_name

TABLE = table_name()

# two coordinates ~20 m apart (same cell at a 202 m side) plus two far-apart cities
_LEEDS = (53.80, -1.50)
_LEEDS_NEARBY = (53.8001, -1.50005)
_MANCHESTER = (53.40, -2.50)


def _connect():
    """A writable in-memory connection, or skip if the spatial extensions can't be fetched."""
    try:
        return duckdb_connector(writeable=True)
    except duckdb.HTTPException as e:  # extension download unavailable
        pytest.skip(f"extension download unavailable: {e}")


def _crime_data(con):
    """Five crime_data rows: three countable, one BTP and one un-geolocated (both excluded), with the
    BNG point ``geom`` the extractor adds (the step reads its coordinates, not lat/lon)."""
    con.execute(f"""
        CREATE OR REPLACE TABLE crime_data AS SELECT *,
            CASE WHEN latitude IS NULL THEN NULL
                 ELSE ST_Transform(ST_Point(longitude, latitude), 'EPSG:4326', 'EPSG:27700', always_xy := true)
            END AS geom
        FROM (VALUES
            ({_LEEDS[0]}, {_LEEDS[1]}, 'Burglary', '2024-01', 'West Yorkshire Police'),
            ({_LEEDS_NEARBY[0]}, {_LEEDS_NEARBY[1]}, 'Burglary', '2024-01', 'West Yorkshire Police'),
            ({_MANCHESTER[0]}, {_MANCHESTER[1]}, 'Bicycle theft', '2024-02', 'Greater Manchester Police'),
            ({_LEEDS[0]}, {_LEEDS[1]}, 'Robbery', '2024-01', 'British Transport Police'),
            (NULL, NULL, 'Public order', '2024-03', 'West Yorkshire Police')
        ) t(latitude, longitude, crime_type, _month, falls_within)
    """)


def _spatial_id(lat: float, lon: float) -> str:
    """The spatial_id the step should emit for a point, via beahiv's scalar encoder."""
    return f"{latlon_to_cell(lat, lon, SIDE_LENGTH, ORIENTATION):016x}"


def test_counts_conserve_filtered_input():
    """Every geolocated non-BTP crime lands in exactly one cell, so the counts sum back to the input."""
    con = _connect()
    _crime_data(con)

    beahiv_counts.build(con, [9], True)

    total = con.execute(f"SELECT SUM(count) FROM {TABLE}").fetchone()[0]
    assert total == 3  # 5 rows − 1 BTP − 1 un-geolocated
    assert beahiv_counts.outputs(con, [9]) == [TABLE]


def test_cell_ids_match_beahiv_scalar_encoder():
    """The vectorised UDF agrees with beahiv's own per-point encoding, and nearby points share a cell."""
    con = _connect()
    _crime_data(con)

    beahiv_counts.build(con, [9], True)

    per_cell = dict(con.execute(f"SELECT spatial_id, SUM(count) FROM {TABLE} GROUP BY spatial_id").fetchall())
    assert per_cell == {
        _spatial_id(*_LEEDS): 2,  # the two Leeds points are ~20 m apart: one cell at a 202 m side
        _spatial_id(*_MANCHESTER): 1,
    }


def test_spatial_id_decodes_to_the_declared_grid():
    """spatial_id is 16-char hex that round-trips to a cell on the configured side length/orientation."""
    con = _connect()
    _crime_data(con)

    beahiv_counts.build(con, [9], True)

    ids = [row[0] for row in con.execute(f"SELECT spatial_id FROM {TABLE}").fetchall()]
    assert all(len(i) == 16 and i == i.lower() for i in ids)
    for cell in cell_ids(ids):
        index = decode(int(cell))
        assert index.side_length == SIDE_LENGTH
        assert index.orientation is ORIENTATION


def test_counts_keyed_by_crime_type_and_month():
    """Same schema as crime_counts_h3_*: one row per (cell, crime type, month)."""
    con = _connect()
    _crime_data(con)

    beahiv_counts.build(con, [9], True)

    rows = con.execute(f"SELECT crime_type, month, count FROM {TABLE} ORDER BY crime_type").fetchall()
    assert rows == [("Bicycle theft", "2024-02", 1), ("Burglary", "2024-01", 2)]


def test_build_is_idempotent():
    """Rebuilding re-registers the UDF rather than failing on the name already being in the catalog."""
    con = _connect()
    _crime_data(con)

    beahiv_counts.build(con, [9], True)
    beahiv_counts.build(con, [9], True)

    assert con.execute(f"SELECT SUM(count) FROM {TABLE}").fetchone()[0] == 3


def test_udf_matches_beahiv_across_a_multi_vector_scan():
    """Over more rows than DuckDB's 2048-row vector, so the UDF is called repeatedly and each returned
    vector must line up with its input — a misaligned or short return would show as mismatched ids."""
    con = _connect()
    beahiv_counts.register_udf(con)
    con.create_function(
        "beahiv_cell_scalar",
        lambda x, y: bng_to_cell(x, y, SIDE_LENGTH, ORIENTATION),
        [DOUBLE, DOUBLE],
        UBIGINT,
    )

    mismatches = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT 400000.0 + i * 37.0 AS x, 400000.0 + i * 53.0 AS y FROM range(5000) t(i)
        ) WHERE beahiv_cell_from_bng(x, y) != beahiv_cell_scalar(x, y)
    """).fetchone()
    assert mismatches == (0,)


def test_unencodable_geometry_raises():
    """A crime that passes the filter but has no BNG point can't be placed in a cell.

    beahiv encodes a missing coordinate to INVALID_CELL_ID rather than raising, so this would
    otherwise be a silently bogus cell — and the conservation check wouldn't notice, because the
    crime is counted, just in the wrong place.
    """
    con = _connect()
    _crime_data(con)
    con.execute(f"INSERT INTO crime_data VALUES ({_LEEDS[0]}, {_LEEDS[1]}, 'Burglary', '2024-01', 'WYP', NULL)")

    with pytest.raises(ValueError, match="did not encode to a cell"):
        beahiv_counts.build(con, [9], True)


def test_conservation_check_raises_on_lossy_aggregation():
    """A silently lossy aggregation raises rather than emitting a skewed grid."""
    con = _connect()
    _crime_data(con)

    class _DropBurglary:
        def __init__(self, con):
            self._con = con

        def __getattr__(self, name):
            return getattr(self._con, name)

        def execute(self, sql, *args, **kwargs):
            if "GROUP BY" in sql:
                sql = sql.replace("GROUP BY", "AND crime_type != 'Burglary' GROUP BY")
            return self._con.execute(sql, *args, **kwargs)

    with pytest.raises(ValueError, match="not conserved"):
        beahiv_counts.build(_DropBurglary(con), [9], True)  # ty:ignore[invalid-argument-type]


def test_scalar_and_vector_bng_to_cell_agree():
    """Guards the assumption the step rests on: beahiv's vectorised bng_to_cell overload returns what
    the scalar one does, so the UDF is a faithful batching of it."""
    xs = [400000.0, 383000.0, 530000.0]
    ys = [400000.0, 398000.0, 180000.0]
    vector = bng_to_cell(xs, ys, SIDE_LENGTH, Orientation.FLAT)
    assert [int(c) for c in vector] == [
        bng_to_cell(x, y, SIDE_LENGTH, Orientation.FLAT) for x, y in zip(xs, ys, strict=True)
    ]
