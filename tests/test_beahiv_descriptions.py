"""Tests for ``beahiv202_descriptions`` / ``hotspots_descriptions``: the human-readable labels built from the
geogs and their lookups.

The contract: every cell of ``beahiv202_geogs`` gets exactly one row with a non-empty ``short_location``
("main road / second road, own LAD", else the LSOA name) and ``description``; names are resolved from the
lookups, a missing name source drops its clause instead of failing the build, and retail centres are not
described even when the geogs carry the (deprecated, opt-in) nearest-centre columns: shops in the cell are
counted instead. Synthetic tables only —
offline-safe.
"""

import duckdb
import pytest
from safer_streets_core.database import duckdb_connector

from safer_streets_tooling.beahiv_grid import KEY
from safer_streets_tooling.transform import STEPS, beahiv, beahiv_descriptions, hotspot_descriptions, hotspots
from safer_streets_tooling.transform.base import Grid

CELL_AREA = 100_000.0
BUSY, EMPTY, PARK = 1, 2, 3  # a town-centre cell, a cell with no named roads, a park with no built-up land


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"""
        CREATE TABLE {KEY}_geogs AS SELECT * FROM (VALUES
            ({BUSY}::BIGINT, {CELL_AREA}, 'L1', 'M1', 'S1', 60000.0, 30000.0, 7.0, 150.0),
            ({EMPTY}, {CELL_AREA}, 'L2', 'M2', 'S2', NULL, NULL, NULL, NULL),
            ({PARK}, {CELL_AREA}, 'L1', 'M1', 'S3', NULL, 5000.0, 7.0, 600.0)
        ) t(spatial_id, cell_area, lad24cd, msoa21cd, lsoa21cd, urban_overlap_area, suburban_overlap_area,
            retail_centre_id, retail_centre_distance);
        CREATE TABLE local_authority_districts AS
            SELECT * FROM (VALUES ('L1', 'Bristol, City of'), ('L2', 'Wiltshire')) t(spatial_id, lad24nm);
        CREATE TABLE msoa_2021 AS
            SELECT * FROM (VALUES ('M1', 'Bristol 008'), ('M2', 'Wiltshire 005')) t(spatial_id, msoa21nm);
        CREATE TABLE lsoa_2021 AS SELECT * FROM (VALUES ('S1', 'Bristol 008A'), ('S2', 'Wiltshire 005B'),
            ('S3', 'Bristol 008C')) t(spatial_id, lsoa21nm);
        CREATE TABLE open_roads AS SELECT * FROM (VALUES
            ('r1', 'Park Road', NULL), ('r2', 'High Street', 'A4'), ('r3', 'High Street', 'A4'),
            ('r4', 'Mews Lane', NULL), ('r5', NULL, 'B3000')
        ) t(id, name_1, road_classification_number);
        -- Park Road is longest, but High Street is an A road split over two links: it wins on weight
        CREATE TABLE {KEY}_road_network_lookup AS SELECT * FROM (VALUES
            ({BUSY}::BIGINT, 'r1', 'Local Road', 300.0), ({BUSY}, 'r2', 'A Road', 100.0),
            ({BUSY}, 'r3', 'A Road', 80.0), ({BUSY}, 'r4', 'Local Road', 50.0),
            ({PARK}, 'r5', 'B Road', 40.0)
        ) t(spatial_id, road_id, type, overlap_length);
        CREATE TABLE open_greenspace AS SELECT * FROM (VALUES
            ('g1', 'Castle Park'), ('g2', 'Castle Park'), ('g3', NULL)
        ) t(id, distName1);
        CREATE TABLE {KEY}_greenspace_lookup AS SELECT * FROM (VALUES
            ({BUSY}::BIGINT, 'g1', 'Public Park Or Garden', 15000.0),
            ({BUSY}, 'g3', 'Play Space', 90000.0),
            ({PARK}, 'g1', 'Public Park Or Garden', 40000.0), ({PARK}, 'g2', 'Public Park Or Garden', 30000.0)
        ) t(spatial_id, greenspace_id, function, overlap_area);
        CREATE TABLE schools AS SELECT * FROM (VALUES
            ({BUSY}::BIGINT, 'Little Infants', 90), ({BUSY}, 'Big Academy', 900)
        ) t({KEY}_id, establishmentname, schoolcapacity);
        CREATE TABLE retail_centres AS SELECT * FROM (VALUES
            (7, 'Union Street; Broadmead; Bristol (South West; England) - 1', 'Regional Centre')
        ) t(rc_id, rc_name, classification);
        -- two shops and a bar in the busy cell, one shop in the park, none in the empty cell
        CREATE TABLE poi AS SELECT * FROM (VALUES
            ({BUSY}::BIGINT, 'convenience_store'), ({BUSY}, 'fashion_and_apparel_store'), ({BUSY}, 'bar'),
            ({PARK}, 'kiosk')
        ) t({KEY}_id, basic_category);
    """)
    beahiv_descriptions.build_unit(con, beahiv.BEAHIV_UNIT, replace=True)
    return con


def _row(con: duckdb.DuckDBPyConnection, cell: int) -> dict:
    df = con.execute(f"SELECT * FROM {KEY}_descriptions WHERE spatial_id = ?", [cell]).df()
    assert len(df) == 1
    return df.iloc[0].to_dict()


def test_one_row_per_geogs_cell(con):
    counts = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT spatial_id) FROM {KEY}_descriptions").fetchone()
    assert counts == (3, 3)


def test_short_location_is_roads_then_own_lad(con):
    """Weighted by road class, High Street (A4, two links) beats the longer Park Road; and the cell's own LAD
    ends the label, normalised from ONS's "Bristol, City of"."""
    row = _row(con, BUSY)
    assert row["road"] == "High Street (A4)"
    assert row["road_pair"] == "High Street / Park Road"
    assert row["roads"] == "Park Road / High Street / Mews Lane"  # by length, for the LLM facts
    assert row["short_location"] == "High Street / Park Road, City of Bristol"


def test_description_names_every_component(con):
    row = _row(con, BUSY)
    assert row["character"] == "Urban"
    assert row["school"] == "Big Academy"  # the larger of the two schools sited in the cell
    assert row["n_shops"] == 2  # the bar is not a shop
    assert row["description"] == "Urban, on High Street (A4), near Big Academy; 2 shops. Bristol 008."


def test_unnamed_greenspace_is_ignored_and_small_named_one_is_not_mentioned(con):
    """g3 covers 90% of the cell but has no name; Castle Park covers 15%, under the 20% threshold."""
    row = _row(con, BUSY)
    assert row["greenspace"] == "Castle Park"
    assert row["greenspace_share"] == pytest.approx(0.15)
    assert "Castle Park" not in row["description"]


def test_park_is_open_space_and_its_split_sites_are_summed(con):
    row = _row(con, PARK)
    assert row["greenspace_share"] == pytest.approx(0.7)
    assert row["character"] == "Open space"
    assert row["road_pair"] == "B3000"  # a numbered road with no name still counts
    assert row["description"] == "Open space, on B3000, in Castle Park; 1 shop. Bristol 008."
    assert row["short_location"] == "B3000, City of Bristol"


def test_cell_without_named_roads_falls_back_to_lsoa(con):
    row = _row(con, EMPTY)
    assert row["character"] == "Rural"
    assert row["n_shops"] == 0  # no shop in the cell: a known zero, and no clause
    assert row["short_location"] == "Wiltshire 005B"
    assert row["description"] == "Rural. Wiltshire 005."


def test_retail_centres_are_not_described(con):
    """The fixture's geogs carry retail_centre_id / retail_centre_distance and a retail_centres table, as an
    opt-in build would: neither reaches the labels or the columns."""
    columns = {name for (name,) in con.execute(f"SELECT column_name FROM (DESCRIBE {KEY}_descriptions)").fetchall()}
    assert not {c for c in columns if "retail" in c}
    for cell in (BUSY, PARK):  # both within the old 800 m description radius of Broadmead
        row = _row(con, cell)
        assert "Broadmead" not in row["description"] + row["short_location"]


def test_missing_name_sources_drop_their_clauses():
    """Only the geogs and the ONS names: no lookups, no schools, no retail centres, no land cover."""
    con = duckdb.connect()
    con.execute(f"""
        CREATE TABLE {KEY}_geogs AS SELECT 1::BIGINT AS spatial_id, 1.0 AS cell_area,
            'L' AS lad24cd, 'M' AS msoa21cd, 'S' AS lsoa21cd;
        CREATE TABLE local_authority_districts AS SELECT 'L' AS spatial_id, 'Leeds' AS lad24nm;
        CREATE TABLE msoa_2021 AS SELECT 'M' AS spatial_id, 'Leeds 045' AS msoa21nm;
        CREATE TABLE lsoa_2021 AS SELECT 'S' AS spatial_id, 'Leeds 045A' AS lsoa21nm;
        CREATE TABLE schools AS SELECT 'x' AS establishmentname;  -- extracted before the cell id existed
    """)
    beahiv_descriptions.build_unit(con, beahiv.BEAHIV_UNIT, replace=True)
    row = _row(con, 1)
    assert row["character"] is None  # unknown without land cover, not "Rural"
    assert row["n_shops"] is None  # unknown without the poi extract, not zero
    assert row["short_location"] == "Leeds 045A"
    assert row["description"] == "Leeds 045."


def test_hotspot_hexes_count_their_shops_by_point_in_polygon():
    """Places carry no hotspot-hex id, so a hex's shops are the shop places inside its polygon."""
    try:
        con = duckdb_connector(writeable=True)
    except duckdb.HTTPException as e:  # extension download unavailable
        pytest.skip(f"extension download unavailable: {e}")
    con.execute("""
        CREATE TABLE hotspots AS SELECT 'hexA' AS spatial_id, ST_Buffer(ST_Point(1000, 1000), 100) AS geom;
        CREATE TABLE hotspots_geogs AS SELECT 'hexA' AS spatial_id, 1.0 AS cell_area,
            'L' AS lad24cd, 'M' AS msoa21cd, 'S' AS lsoa21cd;
        CREATE TABLE local_authority_districts AS SELECT 'L' AS spatial_id, 'Leeds' AS lad24nm;
        CREATE TABLE msoa_2021 AS SELECT 'M' AS spatial_id, 'Leeds 045' AS msoa21nm;
        CREATE TABLE lsoa_2021 AS SELECT 'S' AS spatial_id, 'Leeds 045A' AS lsoa21nm;
        -- two shops and a bar inside the hex, one shop outside it
        CREATE TABLE poi AS SELECT category AS basic_category, ST_Point(x, 1000) AS geom FROM (VALUES
            ('convenience_store', 990), ('bar', 1000), ('kiosk', 1010), ('market', 5000)
        ) t(category, x);
    """)
    beahiv_descriptions.build_unit(con, hotspots.HOTSPOT_UNIT, replace=True)
    row = con.execute("SELECT n_shops, description FROM hotspots_descriptions").df().iloc[0]
    assert row["n_shops"] == 2
    assert row["description"] == "2 shops. Leeds 045."


def test_step_gated_on_the_beahiv_grid_and_registered_after_geogs():
    con = duckdb.connect()
    beahiv_descriptions.build(con, True)  # no crime counts: no grid, so a no-op
    assert beahiv_descriptions.outputs(con) == []
    con.execute(f"CREATE TABLE {beahiv.COUNTS_TABLE} AS SELECT 1::BIGINT AS spatial_id")
    assert beahiv_descriptions.outputs(con) == [f"{KEY}_descriptions"]

    names = [s.name for s in STEPS]
    assert names.index("beahiv_geogs") < names.index("beahiv_descriptions")
    assert set(beahiv_descriptions.STEP.depends_on) == {"beahiv_geogs", "beahiv_lookups"}


def test_hotspot_hexes_are_described_by_the_same_query():
    """The hotspot grid: string hex ids, its own geogs and lookups, and no school (no hex tag on schools)."""
    con = duckdb.connect()
    con.execute(f"""
        CREATE TABLE hotspots_geogs AS SELECT 'hexA' AS spatial_id, 1.0 AS cell_area,
            'L' AS lad24cd, 'M' AS msoa21cd, 'S' AS lsoa21cd;
        CREATE TABLE local_authority_districts AS SELECT 'L' AS spatial_id, 'Leeds' AS lad24nm;
        CREATE TABLE msoa_2021 AS SELECT 'M' AS spatial_id, 'Leeds 045' AS msoa21nm;
        CREATE TABLE lsoa_2021 AS SELECT 'S' AS spatial_id, 'Leeds 045A' AS lsoa21nm;
        CREATE TABLE open_roads AS SELECT 'r1' AS id, 'Briggate' AS name_1, NULL::VARCHAR AS road_classification_number;
        CREATE TABLE hotspots_road_network_lookup AS
            SELECT 'hexA' AS spatial_id, 'r1' AS road_id, 'Local Road' AS type, 100.0 AS overlap_length;
        -- tagged with its BEAHIV cell only, so it must not surface on the hotspot grid
        CREATE TABLE schools AS SELECT 1::BIGINT AS {KEY}_id, 'Big Academy' AS establishmentname,
            900 AS schoolcapacity;
    """)
    beahiv_descriptions.build_unit(con, hotspots.HOTSPOT_UNIT, replace=True)
    row = con.execute("SELECT * FROM hotspots_descriptions").df().iloc[0].to_dict()
    assert row["spatial_id"] == "hexA"
    assert row["road"] == "Briggate"
    assert row["school"] is None
    assert row["short_location"] == "Briggate, Leeds"
    assert row["description"] == "on Briggate. Leeds 045."


def test_hotspot_step_gated_on_the_hexes_and_registered_after_geogs():
    con = duckdb.connect()
    hotspot_descriptions.build(con, True)  # no hotspots extract: a no-op
    assert hotspot_descriptions.outputs(con) == []
    con.execute(f"CREATE TABLE {hotspots.HOTSPOTS_TABLE} AS SELECT 'hexA' AS spatial_id")
    assert hotspot_descriptions.outputs(con) == ["hotspots_descriptions"]

    names = [s.name for s in STEPS]
    assert names.index("hotspot_geogs") < names.index("hotspot_descriptions")
    assert set(hotspot_descriptions.STEP.depends_on) == {"hotspot_geogs", "hotspot_lookups"}
    assert hotspot_descriptions.STEP.grid is Grid.HO
