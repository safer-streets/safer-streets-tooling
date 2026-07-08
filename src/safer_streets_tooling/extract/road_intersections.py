"""OS Open Roads junctions/roundabouts → ``road_intersections.parquet``."""

from zipfile import ZipFile

from safer_streets_core.database import duckdb_connector, write_geoparquet

from safer_streets_tooling.config import data_source
from safer_streets_tooling.extract._common import download, extract_cached, raw_dir, rename_geom_column
from safer_streets_tooling.extract.base import Dataset, ExtractContext

# road_node ``form_of_road_node`` values that are a genuine road intersection (roads meeting), as
# opposed to dead-ends (``road end``) or 2-link attribute-change nodes (``pseudo node``).
INTERSECTION_NODE_TYPES: tuple[str, ...] = ("junction", "roundabout")


def extract(ctx: ExtractContext) -> None:
    """
    Write the ``road_intersections`` parquet from the OS Open Roads ``road_node`` layer.

    Reuses the same GB geopackage as ``open_roads`` (the ``roads`` data source, downloaded from the OS
    Downloads API and cached under the data directory — the same cached zip, so no extra download). Only
    ``road_node`` features that are a genuine intersection are kept: a ``form_of_road_node`` of
    ``junction`` or ``roundabout``, dropping dead-ends (``road end``) and 2-link ``pseudo node``s. As
    with ``open_roads``, the geometry column (native EPSG:27700) is normalised from 'geometry' to 'geom'.
    """
    src = data_source("roads")
    zip_path = raw_dir() / src["zip"]
    if ctx.force_download or not zip_path.exists():
        download(src["url"], zip_path)
    else:
        print(f"  Using cached {zip_path}")

    with ZipFile(zip_path) as z:
        members = [name for name in z.namelist() if name.endswith(src["layer"])]
    if not members:
        raise FileNotFoundError(f"{src['layer']} not found in {zip_path}")

    gpkg = extract_cached(zip_path, members[0])
    con = duckdb_connector(writeable=True)
    try:
        placeholders = ", ".join("?" for _ in INTERSECTION_NODE_TYPES)
        con.execute(
            f"CREATE TABLE road_intersections AS SELECT * FROM ST_Read('{gpkg}', layer='road_node') "
            f"WHERE form_of_road_node IN ({placeholders});",
            list(INTERSECTION_NODE_TYPES),
        )
        rename_geom_column(con, "road_intersections")
        row_count = con.execute("SELECT COUNT(*) FROM road_intersections").fetchone()[0]  # ty:ignore[not-subscriptable]
        write_geoparquet(con, "SELECT * FROM road_intersections", ctx.parquet("road_intersections"))
    finally:
        con.close()
    print(f"  road_intersections: {row_count:,} rows")


DATASET = Dataset(
    name="road_intersections",
    table="road_intersections",
    extract=extract,
    description="OS Open Roads intersection nodes (junctions + roundabouts), for counting per H3 cell.",
)
