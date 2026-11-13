"""Tests for the OSM land-use lookup on the BEAHIV grid: categories, overlap percentages and which cells get rows.

One crime puts one cell on the grid. The land-use polygons are drawn relative to that cell's own outline (beahiv's
``cell_polygon``), so the expected percentages are exact areas over :data:`CELL_AREA`. Synthetic fixtures only —
offline-safe, mirroring test_beahiv_unit.
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

CRIME = (430050.0, 433050.0)  # BNG, central Leeds

COLUMNS = ("osm_id", *(key.replace(":", "_") for key in osm_landuse.LANDUSE_TAGS), "wkt")


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


def _landuse(con: duckdb.DuckDBPyConnection, rows: list[tuple[str, dict[str, str], shapely.Geometry]]) -> None:
    """``osm_landuse`` in the extract's schema: a column per tag (NULL unless given) and a BNG ``geom``."""
    con.execute(f"CREATE TABLE f ({', '.join(f'"{col}" VARCHAR' for col in COLUMNS)})")
    con.executemany(
        f"INSERT INTO f VALUES ({', '.join('?' for _ in COLUMNS)})",
        [[osm_id, *(tags.get(key) for key in osm_landuse.LANDUSE_TAGS), g.wkt] for osm_id, tags, g in rows],
    )
    con.execute("CREATE TABLE osm_landuse AS SELECT * EXCLUDE (wkt), ST_GeomFromText(wkt) AS geom FROM f")
    index_geometry_tables(con)  # as the transform does on import: repaired, labelled BNG, RTree-indexed


def _box(cx: float, cy: float, half: float) -> shapely.Polygon:
    return shapely.box(cx - half, cy - half, cx + half, cy + half)


@pytest.fixture
def lookup() -> pd.DataFrame:
    con = _connect()
    cell = _grid(con)
    cx, cy = cell.centroid.x, cell.centroid.y
    vertex = shapely.Point(cell.exterior.coords[0])
    out_x, out_y = vertex.x - cx, vertex.y - cy  # pointing away from the cell
    # a sliver of a triangle whose tip is the cell's vertex: it touches the cell but covers none of it
    touching = shapely.Polygon(
        [
            (vertex.x, vertex.y),
            (vertex.x + 2 * out_x - 0.1 * out_y, vertex.y + 2 * out_y + 0.1 * out_x),
            (vertex.x + 2 * out_x + 0.1 * out_y, vertex.y + 2 * out_y - 0.1 * out_x),
        ]
    )
    _landuse(
        con,
        [
            ("way/1", {"landuse": "residential"}, _box(cx, cy, 1000)),  # the whole cell, and its neighbours
            ("way/2", {"amenity": "parking", "landuse": "retail"}, _box(cx, cy, 50)),  # parking, not retail
            ("way/3", {"leisure": "pitch"}, _box(cx + 100, cy, 25)),
            ("way/4", {"leisure": "garden"}, _box(cx, cy, 10)),  # no category
            ("way/5", {"shop": "vacant"}, _box(cx, cy, 10)),  # not a shop
            ("way/6", {"highway": "pedestrian", "area:highway": "pedestrian"}, _box(cx - 100, cy, 10)),
            ("relation/7", {"natural": "wood"}, touching),
            ("way/8", {"landuse": "industrial"}, _box(cx + 5000, cy, 100)),  # in a cell without crimes
        ],
    )
    beahiv_osm_landuse.build(con, True)
    return con.execute(f"SELECT * FROM {beahiv_osm_landuse.TABLE}").df()


def test_columns(lookup):
    assert list(lookup.columns) == ["spatial_id", "osm_id", "overlap_pct", "category"]
    assert lookup.spatial_id.nunique() == 1  # the only cell with a crime


def test_overlap_is_the_percentage_of_the_cell_covered(lookup):
    expected = {
        "way/1": 100.0,
        "way/2": 100 * 100**2 / CELL_AREA,
        "way/3": 100 * 50**2 / CELL_AREA,
        "way/6": 100 * 20**2 / CELL_AREA,
    }
    assert lookup.set_index("osm_id").overlap_pct.to_dict() == pytest.approx(expected)


def test_first_matching_category_and_uncategorised_dropped(lookup):
    assert lookup.set_index("osm_id").category.to_dict() == {
        "way/1": "residential",
        "way/2": "parking",
        "way/3": "sport",
        "way/6": "pedestrian",
    }


def test_nested_polygons_each_keep_their_share(lookup):
    """The car park sits on the residential polygon; both keep a row, so a cell's percentages can pass 100."""
    assert lookup.overlap_pct.sum() > 100


def test_noop_without_the_extract():
    con = _connect()
    _grid(con)
    assert beahiv_osm_landuse.outputs(con) == []
    beahiv_osm_landuse.build(con, True)
    assert not con.execute(f"SELECT * FROM duckdb_tables() WHERE table_name = '{beahiv_osm_landuse.TABLE}'").fetchall()


@pytest.mark.parametrize(
    ("tags", "category"),
    [
        ({"amenity": "pub", "landuse": "retail"}, "hospitality"),
        ({"railway": "station"}, "station"),
        ({"railway": "platform"}, None),  # the platform rule needs osm_buildings
        ({"shop": "supermarket", "landuse": "retail"}, "shop"),
        ({"shop": "disused", "landuse": "retail"}, "retail"),
        ({"office": "company"}, "office"),
        ({"amenity": "school", "landuse": "residential"}, "education"),
        ({"landuse": "healthcare"}, "health"),
        ({"place": "square"}, "pedestrian"),
        ({"leisure": "park", "landuse": "grass"}, "park"),
        ({"amenity": "grave_yard"}, "cemetery"),
        ({"landuse": "railway"}, "transport"),
        ({"landuse": "commercial"}, "commercial"),
        ({"landuse": "brownfield"}, "vacant"),
        ({"natural": "scrub"}, "woodland"),
        ({"landuse": "reservoir"}, "water"),
        ({"natural": "heath", "leisure": "nature_reserve"}, "moorland"),
        ({"landuse": "farmland"}, "agriculture"),
        ({"landuse": "grass"}, None),
    ],
)
def test_category(tags, category):
    cols = ", ".join(f"?::VARCHAR AS {col}" for col in COLUMNS[1:-1])
    values = [tags.get(key) for key in osm_landuse.LANDUSE_TAGS]
    (result,) = (
        duckdb.connect().execute(f"SELECT {beahiv_osm_landuse.CATEGORY} FROM (SELECT {cols})", values).fetchall()
    )
    assert result == (category,)


def test_category_keys_are_extract_columns():
    keys = {key for tags in beahiv_osm_landuse.CATEGORIES.values() for key in tags}
    assert keys <= set(osm_landuse.LANDUSE_TAGS)
