"""Tests for the OSM extract: QuackOSM's tag filter, the split of its polygons into land use and buildings, and which
Geofabrik PBF snapshot gets used.

QuackOSM itself isn't run (it needs a real PBF): ``landuse_geoparquet`` is replaced by a small GeoParquet in the shape
QuackOSM writes — ``feature_id``, a ``tags`` MAP and a WGS-84 ``geometry`` of any type."""

from pathlib import Path

import duckdb
import pandas as pd
import pytest
from safer_streets_core.database import duckdb_connector

from safer_streets_tooling.extract import BY_NAME, osm_landuse
from safer_streets_tooling.extract.base import ExtractContext

# Polygons are drawn in BNG near Leeds and transformed to WGS-84, so the extract's reprojection back can be checked
# against known coordinates and areas.
SQUARE_100M = "POLYGON((430000 433000, 430100 433000, 430100 433100, 430000 433100, 430000 433000))"
PARK = "MULTIPOLYGON(((431000 434000, 431200 434000, 431200 434050, 431000 434050, 431000 434000)))"
PLAZA = "POLYGON((429000 432000, 429030 432000, 429030 432030, 429000 432030, 429000 432000))"
FOOTWAY = "LINESTRING(430000 433000, 430100 433100)"
BENCH = "POINT(430050 433050)"
PLATFORM = "POLYGON((430500 433500, 430700 433500, 430700 433505, 430500 433505, 430500 433500))"
SHOP = "POLYGON((430010 433010, 430030 433010, 430030 433030, 430010 433030, 430010 433010))"

QUACKOSM_ROWS = [
    ("way/1", {"landuse": "residential", "name": "Hyde Park Estate"}, SQUARE_100M),
    ("relation/2", {"leisure": "park", "landuse": "grass", "type": "multipolygon"}, PARK),
    ("way/3", {"highway": "pedestrian", "area": "yes", "area:highway": "pedestrian"}, PLAZA),
    ("way/4", {"highway": "footway"}, FOOTWAY),  # matched a key, but a line isn't land use
    ("node/5", {"amenity": "bench"}, BENCH),  # nor is a point
    ("way/6", {"amenity": "parking_space"}, PLAZA),  # a bay inside a car park: POLYGON_FILTER drops it
    ("way/7", {"railway": "platform", "public_transport": "platform", "layer": "-1"}, PLATFORM),  # an underground one
    ("way/8", {"building": "retail", "shop": "supermarket", "name": "Co-op"}, SHOP),
    ("way/9", {"building": "yes", "landuse": "retail"}, SHOP),  # a building is a building whatever else it carries
    ("way/10", {"building": "no", "amenity": "marketplace"}, PLAZA),  # building=no: not a building
]


def _connect() -> duckdb.DuckDBPyConnection:
    try:
        return duckdb_connector(writeable=True)
    except duckdb.HTTPException as e:  # extension download unavailable
        pytest.skip(f"extension download unavailable: {e}")


@pytest.fixture
def quackosm_output(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
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
    return path


@pytest.fixture
def extracted(tmp_path: Path, quackosm_output: Path) -> ExtractContext:
    ctx = ExtractContext(staging=tmp_path)
    osm_landuse.extract_landuse(ctx)
    osm_landuse.extract_buildings(ctx)
    return ctx


def _read(ctx: ExtractContext, name: str) -> pd.DataFrame:
    """The table without its geometry, indexed on osm_id."""
    return pd.read_parquet(ctx.parquet(name)).drop(columns="geom").set_index("osm_id")


LEVEL_COLUMNS = ["layer", "level", "tunnel", "location"]


def test_landuse_table_has_one_row_per_land_use_polygon(extracted):
    df = _read(extracted, "osm_landuse")
    assert list(df.columns) == [
        "landuse",
        "leisure",
        "amenity",
        "natural",
        "shop",
        "office",
        "railway",
        "public_transport",
        "highway",
        "area_highway",
        "place",
        *LEVEL_COLUMNS,
        "name",
        "area_m2",
    ]
    # the footway line, the bench point and the parking bay are dropped, the buildings are in osm_buildings
    assert sorted(df.index) == ["relation/2", "way/1", "way/10", "way/3", "way/7"]


def test_buildings_table_has_one_row_per_building(extracted):
    df = _read(extracted, "osm_buildings")
    assert list(df.columns) == [
        "building",
        "shop",
        "office",
        "amenity",
        "railway",
        "public_transport",
        "landuse",
        "leisure",
        *LEVEL_COLUMNS,
        "name",
        "area_m2",
    ]
    assert sorted(df.index) == ["way/8", "way/9"]
    assert (df.loc["way/8", "building"], df.loc["way/8", "shop"], df.loc["way/8", "name"]) == (
        "retail",
        "supermarket",
        "Co-op",
    )
    assert df.loc["way/9", "landuse"] == "retail"


def test_every_filter_key_is_kept_not_just_the_first_match(extracted):
    df = _read(extracted, "osm_landuse")
    assert (df.loc["relation/2", "leisure"], df.loc["relation/2", "landuse"]) == ("park", "grass")
    assert df.loc["way/3", "area_highway"] == "pedestrian"
    assert df.loc["way/10", "amenity"] == "marketplace"
    assert df.loc["way/1", "name"] == "Hyde Park Estate"
    assert df.loc["way/1"].drop(["landuse", "name", "area_m2"]).isna().all()


def test_platform_keeps_its_level_tags(extracted):
    df = _read(extracted, "osm_landuse")
    assert (df.loc["way/7", "railway"], df.loc["way/7", "public_transport"]) == ("platform", "platform")
    assert df.loc["way/7", "layer"] == "-1"
    assert df.loc["way/7", ["level", "tunnel", "location"]].isna().all()


def test_area_is_measured_in_bng(extracted):
    area = pd.concat([_read(extracted, "osm_landuse"), _read(extracted, "osm_buildings")]).area_m2
    assert area.to_dict() == pytest.approx(
        {
            "way/1": 10_000,
            "relation/2": 10_000,
            "way/3": 900,
            "way/7": 1_000,
            "way/8": 400,
            "way/9": 400,
            "way/10": 900,
        },
        rel=1e-6,
    )


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        (
            "osm_landuse",
            [
                ("relation/2", "MULTIPOLYGON", 431000, 434000),
                ("way/1", "POLYGON", 430000, 433000),
                ("way/10", "POLYGON", 429000, 432000),
                ("way/3", "POLYGON", 429000, 432000),
                ("way/7", "POLYGON", 430500, 433500),
            ],
        ),
        ("osm_buildings", [("way/8", "POLYGON", 430010, 433010), ("way/9", "POLYGON", 430010, 433010)]),
    ],
)
def test_geometry_is_bng_polygons(extracted, name, expected):
    con = _connect()
    rows = con.execute(f"""
        SELECT osm_id, ST_GeometryType(geom)::VARCHAR, round(ST_XMin(geom)), round(ST_YMin(geom))
        FROM read_parquet('{extracted.parquet(name)}') ORDER BY osm_id
    """).fetchall()
    assert rows == expected


def test_datasets_are_registered_buildings_second():
    assert BY_NAME["osm_landuse"].geometry is True
    assert BY_NAME["osm_buildings"].geometry is True
    assert BY_NAME["osm_buildings"].depends_on == ("osm_landuse",)


@pytest.mark.parametrize(
    ("tags", "building"),
    [
        ({"building": "yes"}, True),
        ({"building": "roof"}, True),
        ({"building": "train_station", "railway": "station"}, True),
        ({"building": "no", "shop": "garden_centre"}, False),
        ({"landuse": "retail"}, False),
    ],
)
def test_building(tags, building):
    query = f"SELECT {osm_landuse.BUILDING} FROM (SELECT ?::MAP(VARCHAR, VARCHAR) AS tags)"
    (result,) = duckdb.connect().execute(query, [tags]).fetchall()
    assert result == (building,)


@pytest.mark.parametrize(
    ("tags", "kept"),
    [
        ({"landuse": "retail"}, True),
        ({"amenity": "pub", "building": "yes"}, True),  # a pub building
        ({"building": "yes"}, True),  # any building
        ({"shop": "supermarket"}, True),
        ({"office": "company"}, True),
        ({"railway": "station"}, True),
        ({"public_transport": "platform"}, True),
        ({"highway": "footway", "railway": "platform"}, True),  # a platform mapped on a footway is a polygon
        ({"highway": "primary"}, True),  # passes the gate, but TAGS_FILTER never selects it: no roads
        ({"highway": "footway"}, False),  # a footway line
        ({"highway": "pedestrian", "area": "yes"}, True),  # a pedestrian area mapped as a closed way
        ({"highway": "pedestrian", "type": "multipolygon"}, True),  # ... or as a multipolygon relation
        ({"highway": "services", "landuse": "retail"}, True),  # an unrelated highway tag doesn't gate a landuse
        ({"highway": "pedestrian", "amenity": "marketplace"}, True),  # nor does a pedestrian one on a land use
        ({"highway": "footway", "leisure": "park"}, True),
        ({"area:highway": "pedestrian"}, True),
    ],
)
def test_sql_filter(tags, kept):
    """QuackOSM embeds SQL_FILTER in its queries over each element's tags MAP; evaluated the same way here."""
    query = f"SELECT {osm_landuse.SQL_FILTER} FROM (SELECT ?::MAP(VARCHAR, VARCHAR) AS tags)"
    (result,) = duckdb.connect().execute(query, [tags]).fetchall()
    assert result == (kept,)


@pytest.mark.parametrize(
    ("tags", "kept"),
    [
        ({"amenity": "parking"}, True),  # the car park stays
        ({"amenity": "parking_space"}, False),  # its bays don't
        ({"amenity": "parking_space", "highway": "emergency_bay"}, False),  # a highway tag isn't land use
        ({"amenity": "parking_space", "landuse": "residential"}, True),  # another land-use key keeps it
        ({"amenity": "parking_space", "natural": "scrub"}, True),
        ({"amenity": "parking_space", "building": "carport"}, True),
        ({"landuse": "quarry"}, True),
    ],
)
def test_polygon_filter(tags, kept):
    query = f"SELECT {osm_landuse.POLYGON_FILTER} FROM (SELECT ?::MAP(VARCHAR, VARCHAR) AS tags)"
    (result,) = duckdb.connect().execute(query, [tags]).fetchall()
    assert result == (kept,)


class FakeHead:
    def __init__(self, url: str):
        self.url = url

    def raise_for_status(self) -> None:
        pass


@pytest.fixture
def raw(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(osm_landuse, "raw_dir", lambda: tmp_path)
    return tmp_path


def _serve(monkeypatch: pytest.MonkeyPatch, resolved: str) -> list[tuple[str, Path]]:
    """Make -latest redirect to ``resolved``; returns the list the (fake) downloads are recorded in."""
    downloads: list[tuple[str, Path]] = []
    monkeypatch.setattr(osm_landuse.requests, "head", lambda url, **kw: FakeHead(resolved))
    monkeypatch.setattr(osm_landuse, "download", lambda url, dest: downloads.append((url, dest)) or dest.touch())
    return downloads


def test_newest_cached_pbf_is_used_without_asking_the_server(raw, monkeypatch):
    for name in ("wales-261001.osm.pbf", "wales-261108.osm.pbf", "england-261201.osm.pbf"):
        (raw / name).touch()
    monkeypatch.setattr(osm_landuse.requests, "head", lambda *a, **kw: pytest.fail("no request expected"))
    assert osm_landuse.osm_pbf("wales") == raw / "wales-261108.osm.pbf"


def test_no_cached_pbf_downloads_the_current_snapshot_under_its_dated_name(raw, monkeypatch):
    url = "https://download.geofabrik.de/europe/united-kingdom/wales-261111.osm.pbf"
    downloads = _serve(monkeypatch, url)
    assert osm_landuse.osm_pbf("wales") == raw / "wales-261111.osm.pbf"
    assert downloads == [(url, raw / "wales-261111.osm.pbf")]


def test_forced_download_skips_a_snapshot_already_on_disk(raw, monkeypatch):
    (raw / "wales-261111.osm.pbf").touch()
    downloads = _serve(monkeypatch, "https://download.geofabrik.de/europe/united-kingdom/wales-261111.osm.pbf")
    assert osm_landuse.osm_pbf("wales", force_download=True) == raw / "wales-261111.osm.pbf"
    assert downloads == []


def test_unexpected_redirect_raises(raw, monkeypatch):
    _serve(monkeypatch, "https://download.geofabrik.de/europe/united-kingdom/wales-latest.osm.pbf")
    with pytest.raises(ValueError, match="wales-YYMMDD"):
        osm_landuse.osm_pbf("wales")
