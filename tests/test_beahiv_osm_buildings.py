"""Tests for the OSM buildings step: one category per building, the underground flag, and the per-cell counts.

The step only reads columns (the extract has already put each building in a cell), so the fixture is a plain table in
the extract's schema without geometry. Nothing is downloaded and no extension is loaded.
"""

import duckdb
import pandas as pd
import pytest

from safer_streets_tooling.extract import osm_landuse
from safer_streets_tooling.transform import beahiv_osm_buildings as step

TAGS = osm_landuse.BUILDING_TAGS
CELL_A, CELL_B = 101, 202

# osm_id, tags, name, area_m2, bh350eef_id
BUILDINGS: list[tuple[str, dict[str, str], str | None, float, int | None]] = [
    ("way/1", {"building": "house"}, None, 60.0, CELL_A),
    ("way/2", {"building": "terrace"}, None, 300.0, CELL_A),
    ("way/3", {"building": "house", "amenity": "pub"}, "The Owl", 200.0, CELL_A),  # its use, not its type
    ("way/4", {"building": "yes"}, None, 100.0, CELL_A),
    ("way/5", {"building": "retail", "shop": "vacant"}, None, 80.0, CELL_B),  # an empty unit is still retail-built
    ("way/6", {"building": "yes", "amenity": "parking", "layer": "-1"}, None, 5000.0, CELL_B),  # underground
    ("way/7", {"building": "bridge"}, None, 40.0, CELL_B),
    ("relation/8", {"building": "yes", "office": "company"}, "Acme", 900.0, None),  # centroid off the grid
]


def _buildings(con: duckdb.DuckDBPyConnection, *, tagged: bool = True) -> None:
    """``osm_buildings`` in the extract's schema, less ``geom`` (and less the cell ids when not ``tagged``)."""
    cols = [f'"{key}" VARCHAR' for key in TAGS]
    con.execute(
        f"CREATE TABLE osm_buildings (osm_id VARCHAR, {', '.join(cols)}, name VARCHAR, area_m2 DOUBLE, "
        "h3r9_id VARCHAR, bh350eef_id BIGINT)"
    )
    con.executemany(
        f"INSERT INTO osm_buildings VALUES ({', '.join('?' for _ in range(len(TAGS) + 5))})",
        [
            [osm_id, *(tags.get(key) for key in TAGS), name, area, "891f1d48177ffff", cell]
            for osm_id, tags, name, area, cell in BUILDINGS
        ],
    )
    if not tagged:
        con.execute("ALTER TABLE osm_buildings DROP COLUMN bh350eef_id")


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    _buildings(con)
    step.build(con, True)
    return con


@pytest.fixture
def categories(con) -> pd.DataFrame:
    return con.execute(f"SELECT * FROM {step.TABLE}").df().set_index("osm_id")


@pytest.fixture
def counts(con) -> pd.DataFrame:
    return con.execute(f"SELECT * FROM {step.COUNTS_TABLE}").df().set_index(["spatial_id", "category"])


def test_one_row_per_building_with_the_tags_condensed(categories):
    assert list(categories.columns) == [
        "category",
        "building",
        "name",
        "area_m2",
        "underground",
        "h3r9_id",
        "bh350eef_id",
    ]
    assert categories.category.to_dict() == {
        "way/1": "residential",
        "way/2": "residential",
        "way/3": "hospitality",
        "way/4": "unspecified",
        "way/5": "shop",
        "way/6": "parking",
        "way/7": "other",
        "relation/8": "office",
    }
    assert categories.loc["way/3", ["building", "name"]].tolist() == ["house", "The Owl"]


def test_underground_flag(categories):
    assert categories.index[categories.underground].tolist() == ["way/6"]


def test_counts_per_cell_and_category(counts):
    """Count and footprint per (cell, category): the underground car park and the building with no cell are left out."""
    assert counts.building_count.to_dict() == {
        (CELL_A, "residential"): 2,
        (CELL_A, "hospitality"): 1,
        (CELL_A, "unspecified"): 1,
        (CELL_B, "shop"): 1,
        (CELL_B, "other"): 1,
    }
    assert counts.footprint_area[(CELL_A, "residential")] == 360.0


def test_outputs_are_both_tables():
    con = duckdb.connect()
    _buildings(con)
    assert step.outputs(con) == ["osm_building_categories", "bh350eef_osm_building_counts"]


@pytest.mark.parametrize("tagged", [None, False])
def test_noop_without_a_tagged_extract(tagged):
    """No extract at all, or one from before it carried the cell ids: nothing is built and nothing is declared."""
    con = duckdb.connect()
    if tagged is not None:
        _buildings(con, tagged=tagged)
    assert step.outputs(con) == []
    step.build(con, True)
    assert not con.execute("SELECT * FROM duckdb_tables() WHERE table_name <> 'osm_buildings'").fetchall()


@pytest.mark.parametrize(
    ("tags", "category"),
    [
        ({"building": "yes", "amenity": "restaurant"}, "hospitality"),
        ({"building": "pub"}, "hospitality"),
        ({"building": "yes", "railway": "station"}, "station"),
        ({"building": "train_station"}, "station"),
        ({"building": "yes", "railway": "signal_box"}, "unspecified"),
        ({"building": "apartments", "shop": "convenience"}, "shop"),
        ({"building": "supermarket"}, "shop"),
        ({"building": "house", "office": "estate_agent"}, "office"),
        ({"building": "yes", "office": "vacant"}, "unspecified"),
        ({"building": "school"}, "education"),
        ({"building": "yes", "amenity": "doctors"}, "health"),
        ({"building": "church"}, "worship"),
        ({"building": "yes", "amenity": "place_of_worship"}, "worship"),
        ({"building": "yes", "amenity": "library"}, "civic"),
        ({"building": "yes", "leisure": "sports_centre"}, "leisure"),
        ({"building": "hotel"}, "hotel"),
        ({"building": "commercial"}, "commercial"),
        ({"building": "yes", "landuse": "retail"}, "commercial"),
        ({"building": "warehouse"}, "industrial"),
        ({"building": "barn"}, "agricultural"),
        ({"building": "semidetached_house"}, "residential"),
        ({"building": "garage"}, "outbuilding"),
        ({"building": "yes", "amenity": "shelter"}, "unspecified"),  # an amenity in no category
        ({"building": "ruins"}, "other"),
    ],
)
def test_category(tags, category):
    cols = ", ".join(f'?::VARCHAR AS "{key}"' for key in TAGS)
    values = [tags.get(key) for key in TAGS]
    assert duckdb.connect().execute(f"SELECT {step.CATEGORY} FROM (SELECT {cols})", values).fetchall() == [(category,)]


@pytest.mark.parametrize(
    ("tags", "underground"),
    [
        ({}, False),
        ({"layer": "-1"}, True),
        ({"layer": "1"}, False),
        ({"level": "-2"}, True),
        ({"level": "-1;0"}, False),  # spans the ground floor
        ({"tunnel": "building_passage"}, True),
        ({"tunnel": "no"}, False),
        ({"location": "underground"}, True),
        ({"location": "roof"}, False),
    ],
)
def test_underground(tags, underground):
    cols = ", ".join(f'?::VARCHAR AS "{key}"' for key in TAGS)
    values = [tags.get(key) for key in TAGS]
    assert duckdb.connect().execute(f"SELECT {step.UNDERGROUND} FROM (SELECT {cols})", values).fetchall() == [
        (underground,)
    ]


def test_category_keys_are_extract_columns():
    keys = {key for tags in step.CATEGORIES.values() for key in tags}
    assert keys <= set(TAGS)
