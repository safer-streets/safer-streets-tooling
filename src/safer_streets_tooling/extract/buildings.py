"""Verisk UKBuildings footprints → ``buildings.parquet``."""

from zipfile import ZipFile

from safer_streets_core.database import duckdb_connector, write_geoparquet

from safer_streets_tooling.config import data_source
from safer_streets_tooling.extract._common import extract_cached, raw_dir
from safer_streets_tooling.extract.base import Dataset, ExtractContext

# Attribute columns kept from the source (geometry handled separately); verisk_premise_id is the
# stable per-premise id used to de-duplicate the overlapping download chunks.
KEEP_COLS = ("verisk_premise_id", "premise_use", "premise_type", "uprn", "toid", "map_use", "map_simple_use")

# Resolution of the H3 cell (``h3_9_id``) tagged onto each building; matches the ``building_counts``
# transform and the crime grid (``crime_counts_h3_9`` / ``h3_9_geogs``).
H3_RESOLUTION = 9


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

    Each de-duplicated building is then spatially joined to the 2021 output areas, tagging it with
    ``oa21cd`` (the OA21 code) of the OA whose polygon contains the footprint's *centroid*. Both
    geometries are BNG, so the join is direct. Placing by centroid means every footprint maps to exactly
    one OA (a point falls in at most one non-overlapping polygon), so boundary-straddling footprints are
    assigned cleanly rather than dropped. The join is a LEFT join so no building is ever dropped: a
    footprint whose centroid falls outside every OA (genuinely outside the England & Wales extent, e.g.
    Scotland or offshore structures) is kept with a null ``oa21cd``, and the count is reported.

    The same centroid (reprojected to WGS-84) is indexed to a resolution-9 H3 cell as ``h3_9_id``
    (lowercase hex), matching the ``building_counts`` transform and the crime grid so a consumer can join
    straight onto ``crime_counts_h3_9`` / ``h3_9_geogs``.

    Observed on the full extract (8 GeoPackages): 26,790,009 buildings, of which 158,976 (~0.6%) have a
    centroid in no output area — consistent with footprints lying outside the OA-clipped England & Wales
    extent.
    """
    src = data_source("buildings")
    zips = sorted(raw_dir().glob(src["glob"]))
    if not zips:
        raise FileNotFoundError(
            f"No Verisk UKBuildings download zips matching {src['glob']!r} found under {raw_dir()}.\n"
            f"Download the UKBuildings tiles from EDINA Digimap (https://digimap.edina.ac.uk/) and place the "
            f"Download_<n>_*.zip files in the data directory's raw folder."
        )

    oa_pq = ctx.parquet("output_areas_2021")
    if not oa_pq.exists():
        raise FileNotFoundError(
            f"Output areas parquet not found at {oa_pq}; the 'output_areas_2021' boundary dataset must be "
            f"extracted before buildings (it provides the oa21cd for the spatial join)."
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
            WITH deduped AS (
                SELECT DISTINCT ON (verisk_premise_id) {cols}, geom
                FROM ({union})
            ),
            located AS (
                SELECT
                    {cols}, geom,
                    ST_Centroid(geom) AS centroid,  -- BNG, for the OA containment join
                    ST_Transform(ST_Centroid(geom), 'EPSG:27700', 'EPSG:4326', always_xy := true) AS centroid_ll
                FROM deduped
            )
            SELECT
                b.* EXCLUDE (geom, centroid, centroid_ll),
                oa.spatial_id AS oa21cd,
                lower(hex(h3_latlng_to_cell(ST_Y(b.centroid_ll), ST_X(b.centroid_ll), {H3_RESOLUTION}))) AS h3_9_id,
                b.geom
            FROM located b
            LEFT JOIN read_parquet('{oa_pq}') oa
            ON ST_Contains(oa.geom, b.centroid);
        """)
        row_count, no_oa = con.execute(
            "SELECT COUNT(*), COUNT(*) FILTER (WHERE oa21cd IS NULL) FROM buildings"
        ).fetchone()  # ty:ignore[not-iterable]
        write_geoparquet(con, "SELECT * FROM buildings", ctx.parquet("buildings"))
    finally:
        con.close()
    print(f"  buildings: {row_count:,} rows ({no_oa:,} with no output area)")


DATASET = Dataset(name="buildings", table="buildings", extract=extract, depends_on=("output_areas_2021",))
