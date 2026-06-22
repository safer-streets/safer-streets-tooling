"""FSA food-hygiene food & drink venues in England & Wales → ``food_outlets.parquet``.

The Food Standards Agency publishes its Food Hygiene Rating Scheme (FHRS) open data as a single
whole-country CSV. This extractor keeps the food & drink venues — restaurants/cafes, takeaways,
pubs/bars/nightclubs, other catering premises, mobile caterers and hotels/B&Bs (the
``business_type_ids`` in the catalogue) — in England & Wales: ``SchemeType = 'FHRS'`` drops Scotland
(which runs the separate FHIS scheme) and ``BT`` postcodes drop Northern Ireland, keeping only records
with a valid Geocode. The supplied WGS-84 Longitude/Latitude become a point ``geom`` (reprojected to
BNG) plus a resolution-9 ``h3_9_id`` (lowercase hex) for joining to the H3 grid.
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
    src = data_source("food_outlets")
    matches = sorted(raw_dir().glob(src["glob"]))
    if matches and not force_download:
        print(f"  Using cached {matches[-1]}")
        return matches[-1]
    csv_path = raw_dir() / src["csv"]
    download(src["url"], csv_path)
    return csv_path


def extract(ctx: ExtractContext) -> None:
    """
    Write the ``food_outlets`` parquet from the FSA FHRS whole-country CSV.

    Only the catalogue's ``business_type_ids`` (food & drink venues) in England & Wales with a valid
    Geocode are kept; the raw ``BusinessType`` is carried as ``business_type``, the WGS-84
    Longitude/Latitude become a point ``geom`` (reprojected to BNG, EPSG:27700) and a resolution-9 H3
    cell id (``h3_9_id``, lowercase hex). The CSV is downloaded automatically (cached unless
    force_download).
    """
    src = data_source("food_outlets")
    csv_path = _download_fhrs(force_download=ctx.force_download)
    business_type_ids = ", ".join(f"'{tid}'" for tid in src["business_type_ids"])

    con = duckdb_connector(writeable=True)
    try:
        print(f"  Parsing FHRS export {csv_path}…")
        con.execute(f"""
            CREATE TABLE food_outlets AS
            SELECT
                CAST(FHRSID AS BIGINT) AS fhrsid,
                BusinessName AS business_name,
                BusinessType AS business_type,
                PostCode AS postcode,
                RatingValue AS rating_value,
                LocalAuthorityName AS local_authority_name,
                ST_Transform(
                    ST_Point(CAST(Longitude AS DOUBLE), CAST(Latitude AS DOUBLE)),
                    'EPSG:4326', 'EPSG:27700', always_xy := true
                ) AS geom,
                lower(hex(h3_latlng_to_cell(CAST(Latitude AS DOUBLE), CAST(Longitude AS DOUBLE), 9))) AS h3_9_id
            FROM read_csv_auto('{csv_path}', all_varchar=true)
            WHERE BusinessTypeID IN ({business_type_ids})
              AND SchemeType = 'FHRS'                                   -- England, Wales, NI (not Scotland's FHIS)
              AND (PostCode IS NULL OR PostCode NOT LIKE 'BT%')         -- drop Northern Ireland
              AND TRY_CAST(Latitude AS DOUBLE) IS NOT NULL
              AND TRY_CAST(Longitude AS DOUBLE) IS NOT NULL;
        """)
        row_count = con.execute("SELECT COUNT(*) FROM food_outlets").fetchone()[0]  # ty:ignore[not-subscriptable]
        write_geoparquet(con, "SELECT * FROM food_outlets", ctx.parquet("food_outlets"))
    finally:
        con.close()
    print(f"  food_outlets: {row_count:,} rows")


DATASET = Dataset(name="food_outlets", table="food_outlets", extract=extract)
