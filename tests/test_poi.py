"""Tests for the OSM points-of-interest extract: categories, nodes only, tag columns and grid cells.

QuackOSM isn't run: its cached parse (shared with osm_landuse) is replaced by a small GeoParquet in its shape —
``feature_id``, a ``tags`` MAP and a WGS-84 ``geometry``."""

from pathlib import Path

import duckdb
import pandas as pd
import pytest
from safer_streets_core.database import duckdb_connector

from safer_streets_tooling.extract import BY_NAME, osm_landuse, poi
from safer_streets_tooling.extract.base import ExtractContext

PUB = "POINT(430050 433050)"
BUILDING = "POLYGON((430000 433000, 430100 433000, 430100 433100, 430000 433100, 430000 433000))"
FOOTWAY = "LINESTRING(430000 433000, 430100 433100)"

QUACKOSM_ROWS = [
    ("node/1", {"amenity": "pub", "name": "The Owl", "brand": "Sam Smith's", "addr:postcode": "LS1 1AA"}, PUB),
    ("way/2", {"amenity": "pub", "building": "yes", "name": "The Owl"}, BUILDING),  # a polygon: not a POI
    ("node/3", {"amenity": "cafe", "shop": "bakery"}, PUB),  # food first, then shop
    ("node/4", {"shop": "bookmaker"}, PUB),  # gambling, not shop
    ("node/5", {"shop": "vacant"}, PUB),  # not a shop
    ("node/6", {"amenity": "bench"}, PUB),  # no category
    ("node/7", {"amenity": "parking_space"}, PUB),  # one bay: no category
    ("node/8", {"amenity": "parking", "opening_hours": "24/7"}, PUB),  # a car park mapped as a point
    ("node/9", {"office": "company", "name": "Acme"}, PUB),
    ("way/10", {"shop": "convenience"}, FOOTWAY),  # a line
]


def _connect() -> duckdb.DuckDBPyConnection:
    try:
        return duckdb_connector(writeable=True)
    except duckdb.HTTPException as e:  # extension download unavailable
        pytest.skip(f"extension download unavailable: {e}")


@pytest.fixture
def extracted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> pd.DataFrame:
    path = tmp_path / "quackosm.parquet"
    con = _connect()
    con.execute("CREATE TABLE f (feature_id VARCHAR, tags MAP(VARCHAR, VARCHAR), wkt VARCHAR)")
    con.executemany("INSERT INTO f VALUES (?, ?, ?)", QUACKOSM_ROWS)
    con.execute(f"""
        COPY (
            SELECT feature_id, tags,
                   ST_Transform(ST_GeomFromText(wkt), 'EPSG:27700', 'EPSG:4326', always_xy := true) AS geometry
            FROM f
        ) TO '{path}' (FORMAT parquet)
    """)
    monkeypatch.setattr(osm_landuse, "landuse_geoparquet", lambda ctx: path)
    ctx = ExtractContext(staging=tmp_path)
    poi.extract(ctx)
    return (
        _connect()
        .execute(f"""
            SELECT * EXCLUDE (geom), round(ST_X(geom)) AS x, round(ST_Y(geom)) AS y
            FROM read_parquet('{ctx.parquet("poi")}')
        """)
        .df()
        .set_index("osm_id")
    )


def test_only_categorised_nodes_are_kept(extracted):
    assert extracted.category.to_dict() == {
        "node/1": "alcohol",
        "node/3": "food",
        "node/4": "gambling",
        "node/8": "parking",
        "node/9": "office",
    }


def test_columns(extracted):
    assert list(extracted.columns) == [
        "category",
        "amenity",
        "shop",
        "office",
        "leisure",
        "name",
        "brand",
        "postcode",
        "opening_hours",
        "h3r9_id",
        "bh350eef_id",
        "x",
        "y",
    ]
    owl = extracted.loc["node/1"]
    assert (owl["name"], owl["brand"], owl["postcode"]) == ("The Owl", "Sam Smith's", "LS1 1AA")
    assert extracted.loc["node/3", "shop"] == "bakery"  # the raw tags stay, whatever the category
    assert extracted.loc["node/8", "opening_hours"] == "24/7"


def test_points_are_bng_with_grid_cells(extracted):
    assert (extracted.x == 430050).all()
    assert (extracted.y == 433050).all()
    assert extracted.h3r9_id.str.fullmatch("[0-9a-f]{15}").all()
    assert extracted.bh350eef_id.notna().all()


@pytest.mark.parametrize(
    ("tags", "category"),
    [
        ({"amenity": "nightclub"}, "alcohol"),
        ({"amenity": "restaurant"}, "restaurant"),
        ({"amenity": "fast_food"}, "food"),
        ({"amenity": "stripclub"}, "adult"),
        ({"leisure": "adult_gaming_centre"}, "gambling"),
        ({"amenity": "marketplace"}, "market"),
        ({"shop": "yes"}, "shop"),
        ({"amenity": "pharmacy"}, "shop"),
        ({"shop": "disused"}, None),
        ({"office": "vacant"}, None),
        ({"amenity": "hospital"}, "hospital"),
        ({"amenity": "clinic"}, None),
        ({"amenity": "police"}, "police"),
        ({"amenity": "atm"}, "atm"),
        ({"amenity": "parking_space"}, None),
        ({"landuse": "retail"}, None),
    ],
)
def test_category(tags, category):
    query = f"SELECT {poi.CATEGORY} FROM (SELECT ?::MAP(VARCHAR, VARCHAR) AS tags)"
    (result,) = duckdb.connect().execute(query, [tags]).fetchall()
    assert result == (category,)


def test_category_keys_are_ones_quackosm_selects():
    keys = {key for tags in poi.CATEGORIES.values() for key in tags}
    assert keys <= set(osm_landuse.TAGS_FILTER)


def test_runs_after_osm_landuse_to_reuse_its_parse():
    assert BY_NAME["poi"].depends_on == ("osm_landuse",)
