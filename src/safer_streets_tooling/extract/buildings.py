"""Verisk UKBuildings footprints → ``buildings.parquet``."""

from zipfile import ZipFile

from safer_streets_core.database import duckdb_connector, write_geoparquet

from safer_streets_tooling.config import data_source
from safer_streets_tooling.extract._common import extract_cached, raw_dir
from safer_streets_tooling.extract.base import Dataset, ExtractContext

# Attribute columns kept from the source (geometry handled separately); verisk_premise_id is the
# stable per-premise id used to de-duplicate the overlapping download chunks.
KEEP_COLS = ("verisk_premise_id", "premise_use", "premise_type", "uprn", "toid", "map_use", "map_simple_use")


def extract(ctx: ExtractContext) -> None:
    """
    Write the ``buildings`` parquet from the Verisk UKBuildings GeoPackages.

    Licensed (Verisk via EDINA Digimap) so it cannot be auto-downloaded — the eight
    ``Download_<n>_*.zip`` files (each holding a single ``ukbuildings_*.gpkg``) are located under the
    data directory's raw folder. Each gpkg is extracted once and cached beside its zip (random-access
    SQLite is far slower through /vsizip). Geometry is already BNG (EPSG:27700).

    The chunks tile England & Wales and overlap at their boundaries, so the same premise can appear in
    more than one file; rows are de-duplicated on ``verisk_premise_id`` (matching the notebook). Kept
    columns are the premise/use classification fields plus the building footprint ``geom``.
    """
    src = data_source("buildings")
    zips = sorted(raw_dir().glob(src["glob"]))
    if not zips:
        raise FileNotFoundError(
            f"No Verisk UKBuildings download zips matching {src['glob']!r} found under {raw_dir()}.\n"
            f"Download the UKBuildings tiles from EDINA Digimap (https://digimap.edina.ac.uk/) and place the "
            f"Download_<n>_*.zip files in the data directory's raw folder."
        )

    gpkgs = []
    for zip_path in zips:
        with ZipFile(zip_path) as z:
            members = [name for name in z.namelist() if name.endswith(".gpkg")]
        if not members:
            raise FileNotFoundError(f"No .gpkg found in {zip_path}")
        gpkgs.append(extract_cached(zip_path, members[0]))

    print(f"  Loading buildings from {len(gpkgs)} GeoPackage(s)…")
    con = duckdb_connector(writeable=True)
    try:
        # The geometry column name is consistent across the chunks; discover it from the first file.
        geom_col = con.execute(
            f"SELECT column_name FROM (DESCRIBE SELECT * FROM ST_Read('{gpkgs[0]}')) "
            "WHERE column_type LIKE 'GEOMETRY%' LIMIT 1"
        ).fetchone()[0]  # ty:ignore[not-subscriptable]

        cols = ", ".join(KEEP_COLS)
        union = " UNION ALL ".join(f"SELECT {cols}, \"{geom_col}\" AS geom FROM ST_Read('{gpkg}')" for gpkg in gpkgs)
        con.execute(f"""
            CREATE TABLE buildings AS
            SELECT DISTINCT ON (verisk_premise_id) {cols}, geom
            FROM ({union});
        """)
        row_count = con.execute("SELECT COUNT(*) FROM buildings").fetchone()[0]  # ty:ignore[not-subscriptable]
        write_geoparquet(con, "SELECT * FROM buildings", ctx.parquet("buildings"))
    finally:
        con.close()
    print(f"  buildings: {row_count:,} rows")


DATASET = Dataset(name="buildings", table="buildings", extract=extract)
