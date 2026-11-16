"""Tests for the OSM buildings lookup on the BEAHIV grid: categories, overlap percentages and which cells get rows.

One crime puts one cell on the grid. The footprints are drawn relative to that cell's own outline (beahiv's
``cell_polygon``), so the expected percentages are exact areas over :data:`CELL_AREA`. Synthetic fixtures only —
offline-safe, mirroring test_beahiv_osm_landuse.
"""

import duckdb
import pandas as pd
import pytest
import shapely
from beahiv import cell_polygon
from safer_streets_core.database import duckdb_connector, index_geometry_tables

from safer_streets_tooling.beahiv_grid import CELL_AREA
from safer_streets_tooling.extract import osm_landuse
from safer_streets_tooling.transform import beahiv, beahiv_counts, beahiv_osm_landuse
from safer_streets_tooling.transform import beahiv_osm_buildings as step

CRIME = (430050.0, 433050.0)  # BNG, central Leeds
TAGS = osm_landuse.BUILDING_TAGS


def _connect() -> duckdb.DuckDBPyConnection:
    try:
        return duckdb_connector(writeable=True)
    except duckdb.HTTPException as e:  # extension download unavailable
        pytest.skip(f"extension download unavailable: {e}")


def _grid(con: duckdb.DuckDBPyConnection) -> shapely.Polygon:
    """Put one crime on the grid and return the outline of the cell it lands in."""
    x, y = CRIME
    con.execute(f"""
        CREATE TABLE crime_data AS
        SELECT ST_Point({x}, {y}) AS geom, 'Burglary' AS crime_type, '2024-01' AS _month,
               'West Yorkshire Police' AS falls_within, -1.55 AS longitude, 53.8 AS latitude
    """)
    beahiv_counts.build(con, True)
    (spatial_id,) = con.execute(f"SELECT spatial_id FROM {beahiv.COUNTS_TABLE}").fetchone()  # ty:ignore[not-iterable]
    return cell_polygon(spatial_id)


def _buildings(con: duckdb.DuckDBPyConnection, rows: list[tuple[str, dict[str, str], shapely.Geometry]]) -> None:
    """``osm_buildings`` in the extract's schema: a column per tag (NULL unless given) and a BNG ``geom``."""
    columns = ("osm_id", *TAGS, "wkt")
    con.execute(f"CREATE TABLE f ({', '.join(f'"{col}" VARCHAR' for col in columns)})")
    con.executemany(
        f"INSERT INTO f VALUES ({', '.join('?' for _ in columns)})",
        [[osm_id, *(tags.get(key) for key in TAGS), g.wkt] for osm_id, tags, g in rows],
    )
    con.execute("CREATE TABLE osm_buildings AS SELECT * EXCLUDE (wkt), ST_GeomFromText(wkt) AS geom FROM f")
    index_geometry_tables(con)  # as the transform does on import: repaired, labelled BNG, RTree-indexed


def _box(cx: float, cy: float, half: float) -> shapely.Polygon:
    return shapely.box(cx - half, cy - half, cx + half, cy + half)


@pytest.fixture
def lookup() -> pd.DataFrame:
    con = _connect()
    cell = _grid(con)
    cx, cy = cell.centroid.x, cell.centroid.y
    top = max(y for _, y in cell.exterior.coords)  # a flat-topped cell: its top edge is horizontal through (cx, top)
    _buildings(
        con,
        [
            ("way/1", {"building": "house"}, _box(cx, cy, 5)),
            ("way/2", {"building": "house", "amenity": "pub"}, _box(cx + 50, cy, 10)),  # its use, not its type
            ("way/3", {"building": "yes"}, _box(cx - 50, cy, 10)),
            ("way/4", {"building": "bridge"}, _box(cx, cy + 50, 10)),
            ("way/5", {"building": "yes", "amenity": "parking", "layer": "-1"}, _box(cx, cy - 50, 40)),  # underground
            ("way/6", {"building": "retail"}, _box(cx + 5000, cy, 10)),  # in a cell without crimes
            ("relation/7", {"building": "warehouse"}, _box(cx, top, 10)),  # half in this cell, half in the next
        ],
    )
    step.build(con, True)
    return con.execute(f"SELECT * FROM {step.TABLE}").df()


def test_same_columns_as_the_landuse_lookup(lookup):
    assert list(lookup.columns) == ["spatial_id", "osm_id", "overlap_pct", "category"]
    assert lookup.spatial_id.nunique() == 1  # the only cell with a crime


def test_overlap_is_the_percentage_of_the_cell_the_footprint_covers(lookup):
    """Only the part inside the cell counts: the warehouse straddling the top edge gives half its footprint."""
    expected = {
        "way/1": 100 * 10**2 / CELL_AREA,
        "way/2": 100 * 20**2 / CELL_AREA,
        "way/3": 100 * 20**2 / CELL_AREA,
        "way/4": 100 * 20**2 / CELL_AREA,
        "relation/7": 100 * 20 * 10 / CELL_AREA,
    }
    assert lookup.set_index("osm_id").overlap_pct.to_dict() == pytest.approx(expected)


def test_every_building_above_ground_has_a_category(lookup):
    """Unlike land use nothing is dropped for want of a category; the underground car park (way/5) is left out."""
    assert lookup.set_index("osm_id").category.to_dict() == {
        "way/1": "residential",
        "way/2": "hospitality",
        "way/3": "unspecified",
        "way/4": "other",
        "relation/7": "industrial",
    }


def test_noop_without_the_extract():
    con = _connect()
    _grid(con)
    assert step.outputs(con) == []
    step.build(con, True)
    assert not con.execute(f"SELECT * FROM duckdb_tables() WHERE table_name = '{step.TABLE}'").fetchall()


def test_noop_without_the_grid():
    con = _connect()
    _buildings(con, [("way/1", {"building": "house"}, _box(0, 0, 5))])
    assert step.outputs(con) == []


def test_shared_categories_use_the_landuse_tags():
    """A pub, a shop or a school is the same thing on either lookup: the buildings table only adds building=*."""
    for name in ("hospitality", "station", "shop", "office", "education", "health", "parking"):
        assert beahiv_osm_landuse.CATEGORIES[name].items() <= step.CATEGORIES[name].items()


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
