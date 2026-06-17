import tempfile
from collections import Counter
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock
from zipfile import ZipFile

import duckdb
import geopandas as gpd
import pandas as pd
import pytest
import requests
from safer_streets_core.database import duckdb_connector, read_geoparquet, write_geoparquet
from shapely import LineString, Polygon

from safer_streets_tooling import build_db
from safer_streets_tooling.config import data_source
from safer_streets_tooling.extract import _common, greenspace, imd, land_cover, poi, retail_centres, roads, schools
from safer_streets_tooling.extract.base import Dataset, ExtractContext
from safer_streets_tooling.transform import TransformStep
from safer_streets_tooling.transform.geo_lookups import GEOGRAPHY_MAPPINGS

# source filenames now live in config/data_sources.json (read via data_source); fetch the ones the
# fixtures need so tests stay in step with the catalogue
GREENSPACE_ZIP = data_source("greenspace")["zip"]
LAND_COVER_ZIP = data_source("land_cover")["zip"]
ROADS_ZIP = data_source("roads")["zip"]

# inner path mirrors the real OS bundle layout (…/data/GB_GreenspaceSite.shp)
_INNER_DIR = "OS Open Greenspace (ESRI Shape File) GB/data"


def _connect():
    """A writable in-memory connection, or skip the test if the spatial extensions can't be fetched."""
    try:
        return duckdb_connector(writeable=True)
    except duckdb.HTTPException as e:  # extension download unavailable
        pytest.skip(f"extension download unavailable: {e}")


def _ctx(tmp_path: Path, *, force_download: bool = False) -> ExtractContext:
    return ExtractContext(staging=tmp_path, force_download=force_download)


def _raw(tmp_path: Path) -> Path:
    """Raw-source dir under the (patched) data dir; created so fixtures can write source files into it.

    Extractors read raw inputs from ``raw_dir()`` (``data_dir()/raw``); patch ``_common.data_dir`` to
    ``tmp_path`` so the whole pipeline — every module's ``raw_dir`` — resolves here.
    """
    d = tmp_path / "raw"
    d.mkdir(exist_ok=True)
    return d


def _read_parquet(path: Path):
    """Read a dataset parquet back into a fresh connection (geom returns as GEOMETRY)."""
    con = _connect()
    con.execute(f"CREATE TABLE t AS {read_geoparquet(path)}")
    return con


# --- greenspace ---


def _make_greenspace_zip(zip_path: Path, *, layer: str = "GB_GreenspaceSite") -> None:
    """Build a synthetic OS Open Greenspace bundle zip containing a polygon shapefile."""
    gdf = gpd.GeoDataFrame(
        {"id": ["G1", "G2"], "function": ["Public Park Or Garden", "Play Space"]},
        geometry=[
            Polygon([(0, 0), (100, 0), (100, 100), (0, 100)]),
            Polygon([(200, 200), (300, 200), (300, 300), (200, 300)]),
        ],
        crs="EPSG:27700",
    )
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        gdf.to_file(tmp / f"{layer}.shp")
        with ZipFile(zip_path, "w") as z:
            for f in tmp.iterdir():
                z.write(f, arcname=f"{_INNER_DIR}/{f.name}")


def test_greenspace_raises_when_layer_absent(tmp_path, monkeypatch):
    # a cached zip that lacks the GreenspaceSite layer should raise (before touching a connection)
    monkeypatch.setattr(_common, "data_dir", lambda: tmp_path)
    _make_greenspace_zip(_raw(tmp_path) / GREENSPACE_ZIP, layer="GB_AccessPoint")
    with pytest.raises(FileNotFoundError, match="GB_GreenspaceSite.shp not found"):
        greenspace.extract(_ctx(tmp_path))


def test_greenspace_extracts_to_parquet(tmp_path, monkeypatch):
    monkeypatch.setattr(_common, "data_dir", lambda: tmp_path)
    _make_greenspace_zip(_raw(tmp_path) / GREENSPACE_ZIP)
    _connect().close()  # skip early if extensions unavailable

    greenspace.extract(_ctx(tmp_path))  # zip already cached → no download
    parquet = tmp_path / "open_greenspace.parquet"
    assert parquet.exists()

    con = _read_parquet(parquet)
    cols = [d[0] for d in con.execute("SELECT * FROM t LIMIT 0").description]
    assert "geom" in cols  # ST_Read names the geometry column 'geom', so it gets indexed on assemble
    assert con.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 2
    con.close()


def test_greenspace_downloads_when_zip_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(_common, "data_dir", lambda: tmp_path)
    called = {"n": 0}

    def fake_download(url: str, dest: Path) -> None:
        called["n"] += 1
        _make_greenspace_zip(dest)

    monkeypatch.setattr(greenspace, "download", fake_download)
    _connect().close()

    greenspace.extract(_ctx(tmp_path))
    assert called["n"] == 1  # download triggered because the zip was absent
    assert (tmp_path / "open_greenspace.parquet").exists()


# --- land cover ---


def _gpkg_in_zip(zip_path: Path, gdf: gpd.GeoDataFrame, arcname: str, *, layer: str | None = None) -> None:
    """Write `gdf` to a GeoPackage and pack it into a zip at `arcname` (mirrors the real bundles)."""
    with tempfile.TemporaryDirectory() as td:
        gpkg = Path(td) / "data.gpkg"
        gdf.to_file(gpkg, driver="GPKG", layer=layer) if layer else gdf.to_file(gpkg, driver="GPKG")
        with ZipFile(zip_path, "w") as z:
            z.write(gpkg, arcname=arcname)


def test_land_cover_missing_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(_common, "data_dir", lambda: tmp_path)
    with pytest.raises(FileNotFoundError, match="Land Cover Map GeoPackage not found"):
        land_cover.extract(_ctx(tmp_path))


def test_land_cover_extracts_to_parquet(tmp_path, monkeypatch):
    monkeypatch.setattr(_common, "data_dir", lambda: tmp_path)
    gdf = gpd.GeoDataFrame(
        # two adjacent urban (20) tiles that should dissolve into one polygon, one suburban (21),
        # and a non-built-up class (10) that must be dropped
        {"gid": [1, 2, 3, 4], "_mode": [20, 20, 21, 10]},
        geometry=[
            Polygon([(0, 0), (100, 0), (100, 100), (0, 100)]),
            Polygon([(100, 0), (200, 0), (200, 100), (100, 100)]),
            Polygon([(200, 200), (300, 200), (300, 300), (200, 300)]),
            Polygon([(400, 400), (500, 400), (500, 500), (400, 500)]),
        ],
        crs="EPSG:27700",
    )
    _gpkg_in_zip(_raw(tmp_path) / LAND_COVER_ZIP, gdf, "lcm-2024.gpkg")
    _connect().close()

    land_cover.extract(_ctx(tmp_path))
    con = _read_parquet(tmp_path / "land_cover.parquet")
    cols = [d[0] for d in con.execute("SELECT * FROM t LIMIT 0").description]
    assert {"gid", "urban", "geom"} <= set(cols)
    # non-built-up class dropped; the two urban tiles dissolve to one polygon, suburban stays one
    assert con.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 2
    assert con.execute("SELECT COUNT(*) FROM t WHERE urban").fetchone()[0] == 1
    assert con.execute("SELECT COUNT(*) FROM t WHERE NOT urban").fetchone()[0] == 1
    con.close()


# --- roads ---


def test_roads_extracts_road_link_layer(tmp_path, monkeypatch):
    monkeypatch.setattr(_common, "data_dir", lambda: tmp_path)
    gdf = gpd.GeoDataFrame(
        {"id": ["R1", "R2"], "road_function": ["Local Road", "A Road"]},
        geometry=[LineString([(0, 0), (100, 100)]), LineString([(0, 100), (100, 0)])],
        crs="EPSG:27700",
    )
    # mirror the real bundle: a gpkg at Data/oproad_gb.gpkg with a 'road_link' layer
    _gpkg_in_zip(_raw(tmp_path) / ROADS_ZIP, gdf, "Data/oproad_gb.gpkg", layer="road_link")
    _connect().close()

    roads.extract(_ctx(tmp_path))  # zip cached → no download
    con = _read_parquet(tmp_path / "open_roads.parquet")
    cols = [d[0] for d in con.execute("SELECT * FROM t LIMIT 0").description]
    assert {"id", "road_function", "geom"} <= set(cols)  # geometry column normalised to 'geom'
    assert con.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 2
    con.close()


def test_rename_geom_column_normalises_geometry():
    con = _connect()
    # mimic real OS Open Roads, whose geometry column is named 'geometry'
    con.execute("CREATE TABLE open_roads AS SELECT 1 AS id, ST_Point(0, 0) AS geometry;")
    _common.rename_geom_column(con, "open_roads")
    cols = {d[0] for d in con.execute("SELECT * FROM open_roads LIMIT 0").description}
    assert "geom" in cols and "geometry" not in cols

    # already-'geom' tables are left untouched
    _common.rename_geom_column(con, "open_roads")
    assert "geom" in {d[0] for d in con.execute("SELECT * FROM open_roads LIMIT 0").description}
    con.close()


def test_extract_cached_extracts_reuses_and_refreshes(tmp_path):
    import os
    import time

    zp = tmp_path / "bundle.zip"
    with ZipFile(zp, "w") as z:
        z.writestr("Data/file.gpkg", b"v1")

    out = _common.extract_cached(zp, "Data/file.gpkg")
    assert out == tmp_path / "bundle" / "file.gpkg"
    assert out.read_bytes() == b"v1"

    # second call with an unchanged zip reuses the extracted file (no re-extract)
    mtime = out.stat().st_mtime
    assert _common.extract_cached(zp, "Data/file.gpkg").stat().st_mtime == mtime

    # a newer zip triggers re-extraction
    with ZipFile(zp, "w") as z:
        z.writestr("Data/file.gpkg", b"v2")
    os.utime(zp, (time.time() + 10, time.time() + 10))
    assert _common.extract_cached(zp, "Data/file.gpkg").read_bytes() == b"v2"


# --- POI ---


def test_poi_extracts_filtered_places(tmp_path, monkeypatch):
    """Integration: stream a tiny Overture bbox into the poi parquet (skipped if S3 is unreachable)."""
    _connect().close()
    # a tiny bbox keeps the download small; the module default is all of E&W
    monkeypatch.setattr(poi, "POI_BBOX", (-1.84, 53.91, -1.80, 53.94))
    try:
        poi.extract(_ctx(tmp_path))
    except Exception as e:  # noqa: BLE001 — network/S3 unavailable in this environment
        pytest.skip(f"Overture S3 unavailable: {e}")

    con = _read_parquet(tmp_path / "poi.parquet")
    cols = {d[0] for d in con.execute("SELECT * FROM t LIMIT 0").description}
    assert cols == {"poi_id", "geom", "name", "postcode", "basic_category", "primary_category", "alternate_category"}
    # only the requested categories are kept
    cats = {r[0] for r in con.execute("SELECT DISTINCT basic_category FROM t").fetchall()}
    assert cats <= set(poi.POI_CATEGORIES)
    con.close()


# --- retail centres ---


def test_retail_centres_missing_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(_common, "data_dir", lambda: tmp_path)
    with pytest.raises(FileNotFoundError, match="Retail Centre Boundaries GeoPackage not found"):
        retail_centres.extract(_ctx(tmp_path))


def test_retail_centres_extracts_and_reprojects(tmp_path, monkeypatch):
    monkeypatch.setattr(_common, "data_dir", lambda: tmp_path)
    gdf = gpd.GeoDataFrame(
        {
            "RC_ID": ["RC1", "RC2"],
            "RC_Name": ["Centre A", "Centre B"],
            "Classification": ["Regional Centre", "Local Centre"],
            "Country": ["England", "England"],
            "Region_NM": ["Yorkshire", "Yorkshire"],
            "H3_count": [10, 3],
            "Retail_N": [100, 20],
            "Area_km2": [0.8, 0.2],
        },
        # the GeoDS product is supplied in WGS-84 (lon/lat); use UK coordinates so the
        # reprojection to BNG lands in a valid range
        geometry=[
            Polygon([(-1.5, 53.8), (-1.4, 53.8), (-1.4, 53.9), (-1.5, 53.9)]),
            Polygon([(-1.3, 53.7), (-1.2, 53.7), (-1.2, 53.8)]),
        ],
        crs="EPSG:4326",
    )
    gdf.to_file(_raw(tmp_path) / data_source("retail_centres")["gpkg"], driver="GPKG")
    _connect().close()

    retail_centres.extract(_ctx(tmp_path))
    con = _read_parquet(tmp_path / "retail_centres.parquet")
    cols = {d[0] for d in con.execute("SELECT * FROM t LIMIT 0").description}
    assert {"rc_id", "rc_name", "classification", "geom"} <= cols  # columns lower-cased, geom indexable
    assert con.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 2
    # geometry reprojected to BNG metres (eastings ~400-500 km), not left as lon/lat degrees
    assert con.execute("SELECT MIN(ST_XMin(geom)) FROM t").fetchone()[0] > 1000
    con.close()


# --- schools / isochrones ---

_SQUARE_ROADS = """
    CREATE TABLE open_roads AS SELECT * FROM (VALUES
        ('A','B',100.0, ST_GeomFromText('LINESTRING(0 0,100 0)')),
        ('B','C',100.0, ST_GeomFromText('LINESTRING(100 0,100 100)')),
        ('C','D',100.0, ST_GeomFromText('LINESTRING(100 100,0 100)')),
        ('D','A',100.0, ST_GeomFromText('LINESTRING(0 100,0 0)'))
    ) AS t(start_node, end_node, length, geom);
"""

_GIAS_HEADER = (
    "urn,establishmentnumber,establishmentname,typeofestablishment_code,typeofestablishment_name,"
    "establishmentstatus_code,phaseofeducation_code,phaseofeducation_name,statutorylowage,statutoryhighage,"
    "schoolcapacity,postcode,urbanrural_code,districtadministrative_code,msoa_code,lsoa_code,easting,northing"
)


def _write_gias_csv(path: Path) -> None:
    rows = [
        "1,1,School A,1,Community,1,4,Secondary,11,18,500,LS1 1AA,A1,E08000035,E02,E01,10,10",
        "2,2,Closed School,1,Community,4,4,Secondary,11,18,500,LS1 1AB,A1,E08000035,E02,E01,50,50",  # status 4 → excluded
    ]
    path.write_text("\n".join([_GIAS_HEADER, *rows]) + "\n")


def _write_square_roads_parquet(path: Path) -> None:
    """Materialise the synthetic square road network as an open_roads parquet (schools' dependency)."""
    con = _connect()
    con.execute(_SQUARE_ROADS)
    write_geoparquet(con, "SELECT * FROM open_roads", path)
    con.close()


def test_walk_isochrones_is_convex_hull_of_reachable_nodes():
    # square graph; a school by node A reaches all four corners within the radius
    edges = pd.DataFrame({"start_node": ["A", "B", "C", "D"], "end_node": ["B", "C", "D", "A"], "length": [100.0] * 4})
    nodes = pd.DataFrame({"node": ["A", "B", "C", "D"], "x": [0, 100, 100, 0], "y": [0, 0, 100, 100]})
    pts = pd.DataFrame({"x": [10.0], "y": [10.0]})

    geoms = schools._walk_isochrones(edges, nodes, pts, radius=1000)
    assert len(geoms) == 1
    assert geoms[0].area == 100 * 100  # convex hull of the four corners


def test_download_gias_uses_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(_common, "data_dir", lambda: tmp_path)
    cached = _raw(tmp_path) / "edubasealldata20990101.csv"
    _write_gias_csv(cached)
    # a cached file is reused without hitting the network
    monkeypatch.setattr(schools, "download", lambda *a, **k: pytest.fail("should not download"))
    assert schools._download_gias() == cached


def test_download_gias_downloads_dated_url(tmp_path, monkeypatch):
    monkeypatch.setattr(_common, "data_dir", lambda: tmp_path)
    urls: list[str] = []

    def fake_download(url, path):
        urls.append(url)
        _write_gias_csv(path)

    monkeypatch.setattr(schools, "download", fake_download)
    today = date.today().strftime("%Y%m%d")
    result = schools._download_gias()
    assert result == _raw(tmp_path) / f"edubasealldata{today}.csv"
    assert urls == [f"https://ea-edubase-api-prod.azurewebsites.net/edubase/downloads/public/edubasealldata{today}.csv"]


def test_download_gias_falls_back_to_yesterday(tmp_path, monkeypatch):
    monkeypatch.setattr(_common, "data_dir", lambda: tmp_path)
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y%m%d")

    def fake_download(url, path):
        if yesterday not in url:
            raise requests.HTTPError("not published yet")
        _write_gias_csv(path)

    monkeypatch.setattr(schools, "download", fake_download)
    assert schools._download_gias() == _raw(tmp_path) / f"edubasealldata{yesterday}.csv"


def test_download_gias_failure_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(_common, "data_dir", lambda: tmp_path)
    monkeypatch.setattr(schools, "download", MagicMock(side_effect=requests.ConnectionError("offline")))
    with pytest.raises(FileNotFoundError, match="Could not download the GIAS"):
        schools._download_gias()


def test_schools_requires_open_roads_parquet(tmp_path, monkeypatch):
    monkeypatch.setattr(_common, "data_dir", lambda: tmp_path)
    _write_gias_csv(_raw(tmp_path) / "edubasealldata20990101.csv")
    # no open_roads.parquet in the staging dir → schools cannot build isochrones
    with pytest.raises(RuntimeError, match="require the open_roads parquet"):
        schools.extract(_ctx(tmp_path))


def test_schools_builds_isochrones(tmp_path, monkeypatch):
    monkeypatch.setattr(_common, "data_dir", lambda: tmp_path)
    _write_gias_csv(_raw(tmp_path) / "edubasealldata20990101.csv")
    _connect().close()
    _write_square_roads_parquet(tmp_path / "open_roads.parquet")

    schools.extract(_ctx(tmp_path))
    con = _read_parquet(tmp_path / "schools.parquet")
    cols = {d[0] for d in con.execute("SELECT * FROM t LIMIT 0").description}
    assert {"urn", "geom", "isochrone", "isochrone_area_km2"} <= cols
    # H3 cell ids (resolutions 8-11) derived from the school location
    assert {"h3_8_id", "h3_9_id", "h3_10_id", "h3_11_id"} <= cols
    assert con.execute("SELECT COUNT(*) FROM t WHERE h3_9_id IS NULL").fetchone()[0] == 0
    # the closed school (status 4) is filtered out
    assert con.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 1
    # the isochrone is a polygon with positive area (the reachable square)
    assert con.execute("SELECT ST_GeometryType(isochrone) FROM t").fetchone()[0] == "POLYGON"
    assert con.execute("SELECT isochrone_area_km2 FROM t").fetchone()[0] > 0
    con.close()


# --- IMD (English IoD + Welsh WIMD) ---


def _write_iod_csv(path: Path) -> None:
    import csv

    # csv.writer quotes fields containing commas (some IoD column names contain a comma); seven
    # trailing values cover the seven domain score columns kept in IMD_COLUMNS
    n_scores = len(imd.IMD_COLUMNS) - 5  # minus spatial_id, lad24cd, lad24nm, imd_score, imd_rank
    rows = [
        ["E01000001", "E09000001", "City of London", 5.0, 100, *([0.1] * n_scores)],
        ["E01000002", "E09000001", "City of London", 15.0, 50, *([0.2] * n_scores)],
        ["E01000003", "E09000001", "City of London", 25.0, 10, *([0.3] * n_scores)],
    ]
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(list(imd.IMD_COLUMNS))  # original long IoD column names
        writer.writerows(rows)


def _write_wimd_ods(path: Path) -> None:
    # WIMD "Data" sheet: three preamble rows, then headers, then one row per LSOA. Higher score = more
    # deprived (same convention as the English IoD).
    wimd = pd.DataFrame(
        {
            "LSOA code": ["W01000001", "W01000002", "W01000003"],
            "LSOA name": ["A", "B", "C"],
            "Local Authority name": ["Cardiff", "Cardiff", "Swansea"],
            "WIMD 2025": [10.0, 20.0, 30.0],
            "Income": [1.0, 2.0, 3.0],
            "Employment": [1.0, 2.0, 3.0],
            "Education": [1.0, 2.0, 3.0],
            "Health": [1.0, 2.0, 3.0],
            "Community Safety": [1.0, 2.0, 3.0],
            "Physical Environment": [1.0, 2.0, 3.0],
            "Access to Services": [1.0, 2.0, 3.0],
            "Housing": [1.0, 2.0, 3.0],
        }
    )
    wimd.to_excel(path, sheet_name="Data", startrow=3, index=False, engine="odf")


def test_imd_england_downloads_when_missing(tmp_path, monkeypatch):
    # with no cached CSV present, the English loader fetches it via download (here faked)
    monkeypatch.setattr(_common, "data_dir", lambda: tmp_path)
    monkeypatch.setattr(imd, "download", lambda url, path: _write_iod_csv(path))
    monkeypatch.setattr(imd, "_imd_wales", lambda *a, **k: pd.DataFrame(columns=list(imd.IMD_COLUMNS.values())))
    _connect().close()

    imd.extract(_ctx(tmp_path))
    con = _read_parquet(tmp_path / "imd_scores_pct.parquet")
    assert con.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 3
    assert (_raw(tmp_path) / data_source("imd")["csv"]).exists()  # the (faked) download was cached
    con.close()


def test_imd_extracts_per_lsoa_percentiles(tmp_path, monkeypatch):
    monkeypatch.setattr(_common, "data_dir", lambda: tmp_path)
    # isolate the English path; the Welsh merge is covered separately
    monkeypatch.setattr(imd, "_imd_wales", lambda *a, **k: pd.DataFrame(columns=list(imd.IMD_COLUMNS.values())))
    _write_iod_csv(_raw(tmp_path) / "File_7_IoD2025_All_Ranks_Scores_Deciles_Population_Denominators.csv")
    _connect().close()

    imd.extract(_ctx(tmp_path))
    con = _read_parquet(tmp_path / "imd_scores_pct.parquet")
    cols = {d[0] for d in con.execute("SELECT * FROM t LIMIT 0").description}
    assert {"spatial_id", "imd_rank", "imd_score", "income", "crime"} <= cols
    # the English sub-domains are dropped (Wales has no equivalent)
    assert {"idac", "idaop", "cyp", "indoors", "outdoors"}.isdisjoint(cols)
    assert con.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 3

    # scores become percentile ranks in (0, 1]; the three distinct imd_scores → 1/3, 2/3, 1
    scores = [r[0] for r in con.execute("SELECT imd_score FROM t ORDER BY spatial_id").fetchall()]
    assert scores == pytest.approx([1 / 3, 2 / 3, 1.0])
    # imd_rank is passed through unchanged (not percentiled)
    ranks = [r[0] for r in con.execute("SELECT imd_rank FROM t ORDER BY spatial_id").fetchall()]
    assert ranks == [100, 50, 10]
    con.close()


def test_imd_merges_england_and_wales(tmp_path, monkeypatch):
    monkeypatch.setattr(_common, "data_dir", lambda: tmp_path)
    _write_iod_csv(_raw(tmp_path) / "File_7_IoD2025_All_Ranks_Scores_Deciles_Population_Denominators.csv")
    _write_wimd_ods(_raw(tmp_path) / data_source("wimd")["ods"])
    # the Welsh LA-name→code lookup reads the lad boundary parquet; without it lad24cd is just null
    _connect().close()

    imd.extract(_ctx(tmp_path))
    con = _read_parquet(tmp_path / "imd_scores_pct.parquet")
    # 3 English + 3 Welsh LSOAs, one shared column set
    assert con.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 6
    assert con.execute("SELECT COUNT(*) FROM t WHERE spatial_id LIKE 'W%'").fetchone()[0] == 3

    # Welsh percentiles are ranked within Wales (higher score = more deprived); most-deprived → imd_rank 1
    wales = con.execute("SELECT imd_score, imd_rank, bhs FROM t WHERE spatial_id = 'W01000003'").fetchone()
    assert wales[0] == pytest.approx(1.0)  # highest WIMD score in Wales → top percentile
    assert wales[1] == 1  # …and rank 1 (most deprived)
    assert wales[2] == pytest.approx(1.0)  # bhs = mean(Access, Housing) percentile, also highest
    con.close()


def test_imd_welsh_lad_codes_from_boundary_parquet(tmp_path):
    """When the lad boundary parquet is present, Welsh rows get their lad24cd looked up by LA name."""
    con = _connect()
    con.execute(
        "CREATE TABLE lad AS SELECT * FROM (VALUES "
        "('W06000015','Cardiff'),('W06000011','Swansea')) AS t(spatial_id, lad24nm);"
    )
    write_geoparquet(con, "SELECT * FROM lad", tmp_path / "local_authority_districts.parquet")
    con.close()

    codes = imd._welsh_lad_codes(_ctx(tmp_path))
    assert codes == {"Cardiff": "W06000015", "Swansea": "W06000011"}
    # absent parquet → empty mapping (lad24cd left null)
    assert imd._welsh_lad_codes(_ctx(tmp_path / "empty")) == {}


# --- orchestrator: extract / transform / load ---


def test_run_extract_skips_cached_unless_rebuild(tmp_path):
    """run_extract keeps a dataset whose parquet exists when rebuild=False, and re-runs when True."""
    calls: list[str] = []

    def fake_extract(ctx):
        calls.append("ran")
        ctx.parquet("d").write_bytes(b"x")

    ds = Dataset(name="d", table="d", extract=fake_extract)
    ctx = _ctx(tmp_path)

    # absent → runs
    build_db.run_extract([ds], ctx, rebuild=False)
    assert calls == ["ran"]
    # present + rebuild False → skipped
    build_db.run_extract([ds], ctx, rebuild=False)
    assert calls == ["ran"]
    # present + rebuild True → re-runs
    build_db.run_extract([ds], ctx, rebuild=True)
    assert calls == ["ran", "ran"]


def test_run_extract_optional_failure_is_skipped_required_propagates(tmp_path):
    ctx = _ctx(tmp_path)
    boom = Dataset(name="opt", table="opt", extract=MagicMock(side_effect=RuntimeError("nope")), optional=True)
    build_db.run_extract([boom], ctx, rebuild=False)  # optional → swallowed

    required = Dataset(name="req", table="req", extract=MagicMock(side_effect=RuntimeError("nope")), optional=False)
    with pytest.raises(RuntimeError, match="nope"):
        build_db.run_extract([required], ctx, rebuild=False)


def test_run_transform_caches_outputs_and_skips_unless_rebuild(tmp_path, monkeypatch):
    """run_transform writes each node's output parquet (not the imported inputs). A second run skips
    nodes whose output parquet already exist (build fn not re-called); ``rebuild`` forces re-execution."""
    con = _connect()
    write_geoparquet(con, "SELECT 1 AS spatial_id, ST_Point(0, 0) AS geom", tmp_path / "req_geom.parquet")
    con.close()

    datasets = (Dataset(name="req_geom", table="req_geom", extract=lambda ctx: None, optional=False),)
    monkeypatch.setattr(build_db, "DATASETS", datasets)

    # stand-in transform steps: each creates the relations it declares as outputs; the overlap/retail
    # steps have no outputs here (so the imported input is never re-written by the transform phase)
    calls: Counter[str] = Counter()

    def fake_step(name, *output_names, depends_on=()):
        def build(con, resolutions, replace):
            calls[name] += 1
            for out in output_names:
                con.execute(f'CREATE TABLE "{out}" AS SELECT 1 AS spatial_id, 2 AS v')

        return TransformStep(name=name, build=build, outputs=lambda con, res: list(output_names), depends_on=depends_on)

    steps = (
        fake_step("crime_counts", "crime_counts_h3_8"),
        fake_step("geo_lookups", *(f"h3_8_{key}_lookup" for key in GEOGRAPHY_MAPPINGS), depends_on=("crime_counts",)),
        fake_step("overlap_lookups", depends_on=("crime_counts",)),
        fake_step("retail_centre_lookups", depends_on=("crime_counts",)),
        fake_step("geogs", "h3_8_geogs", depends_on=("geo_lookups", "overlap_lookups", "retail_centre_lookups")),
    )
    monkeypatch.setattr(build_db, "STEPS", steps)

    tdir = tmp_path / "transform"
    tdir.mkdir()

    build_db.run_transform(tmp_path, tdir, resolutions=[8])
    assert (tdir / "crime_counts_h3_8.parquet").exists()  # crime_counts step (now in transform) wrote its output
    assert (tdir / "h3_8_geogs.parquet").exists()  # geogs step wrote its output
    assert (tdir / "h3_8_lad24cd_lookup.parquet").exists()  # a derived lookup is written
    assert not (tdir / "req_geom.parquet").exists()  # imported inputs are not written by transform
    assert calls["crime_counts"] == 1 and calls["geo_lookups"] == 1 and calls["geogs"] == 1

    build_db.run_transform(tmp_path, tdir, resolutions=[8])  # outputs present → skipped (reloaded)
    assert calls["crime_counts"] == 1 and calls["geo_lookups"] == 1 and calls["geogs"] == 1

    build_db.run_transform(tmp_path, tdir, resolutions=[8], rebuild=True)  # forced → rebuilt
    assert calls["crime_counts"] == 2 and calls["geo_lookups"] == 2 and calls["geogs"] == 2


def test_run_load_builds_minimal_db_with_optional_includes(tmp_path, monkeypatch):
    """run_load imports crime_counts + geogs + the ONS boundary tables by default; --include adds extra
    tables resolved from the transform then the extract dir."""
    con = _connect()
    edir = tmp_path / "extract"
    tdir = tmp_path / "transform"
    edir.mkdir()
    tdir.mkdir()
    # minimal tables (transform outputs, no geometry)
    write_geoparquet(con, "SELECT 'a' AS spatial_id, 5 AS count", tdir / "crime_counts_h3_8.parquet")
    write_geoparquet(con, "SELECT 'a' AS spatial_id, 'L' AS lad24cd", tdir / "h3_8_geogs.parquet")
    # the ONS boundary tables (extract, with geometry) the geogs codes resolve to — part of the minimal set
    boundaries = set(GEOGRAPHY_MAPPINGS.values())
    for table in boundaries:
        write_geoparquet(con, "SELECT 1 AS spatial_id, ST_Point(0, 0) AS geom", edir / f"{table}.parquet")
    # a non-default table: an intermediate lookup (transform), only loaded when included
    write_geoparquet(con, "SELECT 'a' AS spatial_id, 'L' AS lad24cd", tdir / "h3_8_lad24cd_lookup.parquet")
    con.close()

    indexed = []
    monkeypatch.setattr(build_db, "index_geometry_tables", lambda con: indexed.append(True))

    def _tables(db_path):
        out = duckdb_connector(db_path)
        names = {
            r[0]
            for r in out.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        }
        out.close()
        return names

    minimal = {"crime_counts_h3_8", "h3_8_geogs"} | boundaries

    db_path = tmp_path / "out.db"
    build_db.run_load(db_path, tdir, [8], edir=edir)
    assert db_path.exists()
    assert indexed == [True]
    assert _tables(db_path) == minimal  # counts + geogs + boundaries; the lookup is excluded by default

    db2 = tmp_path / "out2.db"
    build_db.run_load(db2, tdir, [8], edir=edir, include=["h3_8_lad24cd_lookup"])
    assert _tables(db2) == minimal | {"h3_8_lad24cd_lookup"}


def test_run_load_missing_required_raises(tmp_path):
    _connect().close()
    tdir = tmp_path / "transform"
    tdir.mkdir()
    # geogs present but crime_counts absent → required minimal table missing
    con = _connect()
    write_geoparquet(con, "SELECT 'a' AS spatial_id, 'L' AS lad24cd", tdir / "h3_8_geogs.parquet")
    con.close()
    with pytest.raises(FileNotFoundError, match="required table 'crime_counts_h3_8' parquet not found"):
        build_db.run_load(tmp_path / "out.db", tdir, [8])
