"""FSA food-hygiene takeaways in England & Wales → ``takeaways.parquet``.

The Food Standards Agency publishes its Food Hygiene Rating Scheme (FHRS) open data as a single
whole-country CSV. This extractor keeps just the *takeaways* (BusinessTypeID 7844) in England & Wales —
``SchemeType = 'FHRS'`` drops Scotland (which runs the separate FHIS scheme) and ``BT`` postcodes drop
Northern Ireland — with a valid Geocode. The supplied WGS-84 Longitude/Latitude become a point ``geom``
(reprojected to BNG) plus a resolution-9 ``h3_9_id`` (lowercase hex) for joining to the H3 grid.
"""

from pathlib import Path

from safer_streets_core.database import duckdb_connector, write_geoparquet

from safer_streets_tooling.config import data_source
from safer_streets_tooling.extract._common import download, raw_dir
from safer_streets_tooling.extract.base import Dataset, ExtractContext


def _download_fhrs(*, force_download: bool = False) -> Path:
    """Return a local path to the FSA whole-country FHRS CSV, downloading it if needed.

    Published at a fixed URL; unless force_download is set, a cached copy (glob ``fhrs_all*.csv``) is
    reused, otherwise it is fetched and cached under the data directory's raw folder.
    """
    src = data_source("takeaways")
    matches = sorted(raw_dir().glob(src["glob"]))
    if matches and not force_download:
        print(f"  Using cached {matches[-1]}")
        return matches[-1]
    csv_path = raw_dir() / src["csv"]
    download(src["url"], csv_path)
    return csv_path


def extract(ctx: ExtractContext) -> None:
    """
    Write the ``takeaways`` parquet from the FSA FHRS whole-country CSV.

    Only takeaways (BusinessTypeID 7844) in England & Wales with a valid Geocode are kept; the WGS-84
    Longitude/Latitude become a point ``geom`` (reprojected to BNG, EPSG:27700) and a resolution-9 H3
    cell id (``h3_9_id``, lowercase hex). The CSV is downloaded automatically (cached unless
    force_download).
    """
    src = data_source("takeaways")
    csv_path = _download_fhrs(force_download=ctx.force_download)

    con = duckdb_connector(writeable=True)
    try:
        print(f"  Parsing FHRS export {csv_path}…")
        con.execute(f"""
            CREATE TABLE takeaways AS
            SELECT
                CAST(FHRSID AS BIGINT) AS fhrsid,
                BusinessName AS business_name,
                trim(concat_ws(', ',
                    nullif(trim(AddressLine1), ''), nullif(trim(AddressLine2), ''),
                    nullif(trim(AddressLine3), ''), nullif(trim(AddressLine4), ''))) AS address,
                PostCode AS postcode,
                RatingValue AS rating_value,
                TRY_CAST(RatingDate AS DATE) AS rating_date,
                TRY_CAST(Hygiene AS INTEGER) AS hygiene_score,
                TRY_CAST(Structural AS INTEGER) AS structural_score,
                TRY_CAST(ConfidenceInManagement AS INTEGER) AS confidence_score,
                LocalAuthorityName AS local_authority_name,
                ST_Transform(
                    ST_Point(CAST(Longitude AS DOUBLE), CAST(Latitude AS DOUBLE)),
                    'EPSG:4326', 'EPSG:27700', always_xy := true
                ) AS geom,
                lower(hex(h3_latlng_to_cell(CAST(Latitude AS DOUBLE), CAST(Longitude AS DOUBLE), 9))) AS h3_9_id
            FROM read_csv_auto('{csv_path}', all_varchar=true)
            WHERE BusinessTypeID = '{src["business_type_id"]}'
              AND SchemeType = 'FHRS'                                   -- England, Wales, NI (not Scotland's FHIS)
              AND (PostCode IS NULL OR PostCode NOT LIKE 'BT%')         -- drop Northern Ireland
              AND TRY_CAST(Latitude AS DOUBLE) IS NOT NULL
              AND TRY_CAST(Longitude AS DOUBLE) IS NOT NULL;
        """)
        row_count = con.execute("SELECT COUNT(*) FROM takeaways").fetchone()[0]  # ty:ignore[not-subscriptable]
        write_geoparquet(con, "SELECT * FROM takeaways", ctx.parquet("takeaways"))
    finally:
        con.close()
    print(f"  takeaways: {row_count:,} rows")


DATASET = Dataset(name="takeaways", table="takeaways", extract=extract)
