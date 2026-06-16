"""GIAS schools with 10-minute walk isochrones → ``schools.parquet`` (needs ``open_roads``)."""

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests
from safer_streets_core.database import duckdb_connector
from safer_streets_core.utils import data_dir

from safer_streets_tooling.config import data_source
from safer_streets_tooling.datasets._common import download, write_geoparquet
from safer_streets_tooling.datasets.base import Dataset, ExtractContext

# Isochrones are 10-minute walk catchments over the open_roads network.
WALK_TRIP_MINUTES = 10
WALK_SPEED_KMH = 5
WALK_RADIUS_M = WALK_TRIP_MINUTES * WALK_SPEED_KMH * 1000 / 60  # reachable network distance, metres


def _walk_isochrones(
    edges: pd.DataFrame,
    nodes: pd.DataFrame,
    school_xy: pd.DataFrame,
    radius: float = WALK_RADIUS_M,
) -> list:
    """
    Return a walk-isochrone geometry per school (aligned with ``school_xy``).

    ``edges`` has start_node/end_node/length (a topological road graph), ``nodes`` has node/x/y, and
    ``school_xy`` has x/y point coordinates (BNG). Each school is snapped to its nearest road node;
    its isochrone is the convex hull of all nodes reachable within ``radius`` metres (bounded
    single-source Dijkstra), or the snap point itself if the node is isolated.
    """
    import networkx as nx  # noqa: PLC0415
    from scipy.spatial import cKDTree  # noqa: PLC0415  # ty:ignore[unresolved-import]
    from shapely.geometry import MultiPoint, Point  # noqa: PLC0415

    graph = nx.from_pandas_edgelist(edges, "start_node", "end_node", edge_attr="length")
    coords = dict(zip(nodes.node, zip(nodes.x, nodes.y, strict=True), strict=True))

    # snap each school to the nearest road node (vectorised KDTree query)
    tree = cKDTree(nodes[["x", "y"]].to_numpy())
    _, idx = tree.query(school_xy[["x", "y"]].to_numpy())
    school_nodes = nodes.node.to_numpy()[idx]

    isochrones = []
    for node in school_nodes:
        if node not in graph:
            isochrones.append(Point(coords[node]))
            continue
        reached = nx.single_source_dijkstra_path_length(graph, node, cutoff=radius, weight="length")
        isochrones.append(MultiPoint([coords[n] for n in reached]).convex_hull)
    return isochrones


def _download_gias(*, force_download: bool = False) -> Path:
    """Return a local path to the GIAS 'all establishment data' CSV, downloading it if needed.

    The export is published daily at a {date}-stamped (YYYYMMDD) URL. Unless force_download is set,
    a cached copy (glob ``edubasealldata*.csv``) is reused; otherwise today's file is fetched, falling
    back to yesterday's if today's has not been published yet.
    """
    src = data_source("schools")
    matches = sorted(data_dir().glob(src["glob"]))
    if matches and not force_download:
        print(f"  Using cached {matches[-1]}")
        return matches[-1]

    for day in (date.today(), date.today() - timedelta(days=1)):
        stamp = day.strftime("%Y%m%d")
        csv_path = data_dir() / src["csv"].format(date=stamp)
        try:
            download(src["url"].format(date=stamp), csv_path)
            return csv_path
        except requests.RequestException as exc:
            print(f"  GIAS export for {stamp} unavailable ({exc}); trying previous day…")
            csv_path.unlink(missing_ok=True)
    raise FileNotFoundError(
        f"Could not download the GIAS schools export from {src['url'].format(date='<date>')}.\n"
        "Check connectivity, or download 'Establishment fields CSV' from "
        "https://get-information-schools.service.gov.uk/Downloads and place it in the data directory."
    )


def extract(ctx: ExtractContext) -> None:
    """
    Write the ``schools`` parquet from the GIAS export, with a 10-minute walk isochrone per school.

    Open schools with valid coordinates are parsed into a point ``geom`` (BNG) plus ``h3_{8..11}_id``
    cell ids (the GIAS export has no lat/lon, so these are derived by transforming geom back to
    WGS-84); a walk catchment ``isochrone`` polygon is then computed over the open_roads network, so
    this requires the ``open_roads`` parquet (extract roads first). The GIAS export is downloaded
    automatically (cached unless force_download).
    """
    roads_pq = ctx.parquet("open_roads")
    if not roads_pq.exists():
        raise RuntimeError("schools isochrones require the open_roads parquet (extract roads first)")
    gias = _download_gias(force_download=ctx.force_download)

    con = duckdb_connector(writeable=True)
    try:
        con.execute(f"CREATE TEMP VIEW open_roads AS SELECT * FROM read_parquet('{roads_pq}');")

        print(f"  Parsing GIAS export {gias}…")
        con.execute(f"""
            CREATE TEMP VIEW schools_stg AS
            WITH base AS (
                SELECT
                    urn, establishmentnumber, establishmentname,
                    typeofestablishment_code, typeofestablishment_name,
                    establishmentstatus_code, phaseofeducation_code, phaseofeducation_name,
                    statutorylowage, statutoryhighage, schoolcapacity, postcode,
                    urbanrural_code, districtadministrative_code, msoa_code, lsoa_code,
                    ST_Point(easting, northing) AS geom
                FROM read_csv_auto('{gias}', encoding='CP1252', normalize_names=true)
                WHERE establishmentstatus_code IN (1, 3) AND easting > 0 AND northing > 0
            ),
            pts AS (
                SELECT
                    *,
                    ST_Transform(geom, 'EPSG:27700', 'EPSG:4326', always_xy := true) AS pt
                FROM base
            )
            SELECT
                * EXCLUDE pt,
                lower(hex(h3_latlng_to_cell(ST_Y(pt), ST_X(pt), 8)))  AS h3_8_id,
                lower(hex(h3_latlng_to_cell(ST_Y(pt), ST_X(pt), 9)))  AS h3_9_id,
                lower(hex(h3_latlng_to_cell(ST_Y(pt), ST_X(pt), 10))) AS h3_10_id,
                lower(hex(h3_latlng_to_cell(ST_Y(pt), ST_X(pt), 11))) AS h3_11_id
            FROM pts;
        """)

        # the road links are already a topological graph (start_node -- end_node, weighted by length)
        edges = con.sql("SELECT start_node, end_node, length FROM open_roads WHERE length > 0").df()
        nodes = con.sql("""
            SELECT DISTINCT ON (node) node, ST_X(pt) AS x, ST_Y(pt) AS y
            FROM (
                SELECT start_node AS node, ST_StartPoint(geom) AS pt FROM open_roads
                UNION ALL
                SELECT end_node AS node, ST_EndPoint(geom) AS pt FROM open_roads
            )
        """).df()
        school_xy = con.sql("SELECT urn, ST_X(geom) AS x, ST_Y(geom) AS y FROM schools_stg").df()

        print(f"  Computing {len(school_xy):,} school isochrones over {len(nodes):,} road nodes…")
        isochrones = _walk_isochrones(edges, nodes, school_xy)

        iso_wkt = pd.DataFrame({"urn": school_xy.urn.to_numpy(), "wkt": [g.wkt for g in isochrones]})
        con.register("schools_iso_tmp", iso_wkt)
        try:
            con.execute("""
                CREATE TABLE schools AS
                SELECT
                    s.*,
                    ST_GeomFromText(t.wkt) AS isochrone,
                    ST_Area(ST_GeomFromText(t.wkt)) / 1e6 AS isochrone_area_km2
                FROM schools_stg s
                LEFT JOIN schools_iso_tmp t USING (urn);
            """)
        finally:
            con.unregister("schools_iso_tmp")
        row_count = con.execute("SELECT COUNT(*) FROM schools").fetchone()[0]  # ty:ignore[not-subscriptable]
        write_geoparquet(con, "SELECT * FROM schools", ctx.parquet("schools"))
    finally:
        con.close()
    print(f"  schools: {row_count:,} rows")


DATASET = Dataset(name="schools", table="schools", extract=extract, depends_on=("open_roads",))
