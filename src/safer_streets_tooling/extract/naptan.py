"""NAPTAN public-transport access nodes (bus stops, rail/metro/tram, ferry, taxi…) → ``naptan.parquet``.

The DfT NAPTAN national export is a single national CSV of every public-transport access node in Great
Britain, keyed by ATCO code. Only *active* stops with valid coordinates are kept; the supplied
Easting/Northing (already BNG, EPSG:27700) become the point ``geom``, and the NAPTAN ``StopType`` is
folded into a coarse ``stop_category`` (bus / rail / tram_metro / ferry / taxi / air / other) for easy
filtering.
"""

from pathlib import Path

from safer_streets_core.database import duckdb_connector, write_geoparquet

from safer_streets_tooling.config import data_source
from safer_streets_tooling.extract._common import download, raw_dir
from safer_streets_tooling.extract.base import Dataset, ExtractContext

# NAPTAN StopType code -> coarse category. Codes not listed fall through to 'other'.
# https://www.gov.uk/government/publications/national-public-transport-access-node-schema
STOP_CATEGORY = {
    "BCT": "bus",  # on-street bus / coach / tram stop
    "BCS": "bus",  # bus / coach station bay
    "BCQ": "bus",  # bus / coach station variable bay
    "BCE": "bus",  # bus / coach station entrance
    "BST": "bus",  # bus / coach trolley station
    "MKD": "bus",  # marked (point) bus stop
    "RLY": "rail",  # rail station access area (platform)
    "RPL": "rail",  # rail platform
    "RSE": "rail",  # rail station entrance
    "MET": "tram_metro",  # tram / metro / underground access area
    "PLT": "tram_metro",  # tram / metro / underground platform
    "TMU": "tram_metro",  # tram / metro / underground station entrance
    "FER": "ferry",  # ferry / port access area
    "FBT": "ferry",  # ferry / port berth
    "FTD": "ferry",  # ferry terminal / dock entrance
    "TXR": "taxi",  # taxi rank
    "STR": "taxi",  # shared taxi rank
    "AIR": "air",  # airport entrance
    "GAT": "air",  # airport interchange area
}


def _download_naptan(*, force_download: bool = False) -> Path:
    """Return a local path to the NAPTAN national CSV, downloading it if needed.

    The export is published at a fixed URL; unless force_download is set, a cached copy (glob
    ``naptan*.csv``) is reused, otherwise it is fetched and cached under the data directory's raw folder.
    """
    src = data_source("naptan")
    matches = sorted(raw_dir().glob(src["glob"]))
    if matches and not force_download:
        print(f"  Using cached {matches[-1]}")
        return matches[-1]
    csv_path = raw_dir() / src["csv"]
    download(src["url"], csv_path)
    return csv_path


def extract(ctx: ExtractContext) -> None:
    """
    Write the ``naptan`` parquet from the DfT NAPTAN national CSV (public-transport access nodes).

    Only active stops with a valid Easting/Northing are kept; the supplied BNG coordinates become a
    point ``geom`` (EPSG:27700) and the raw ``StopType`` is mapped to a coarse ``stop_category``. A
    resolution-9 H3 cell id (``h3_9_id``, lowercase hex) is derived from ``geom`` (transformed to
    WGS-84) for joining to the H3 grid — using ``geom`` rather than the CSV's own Longitude/Latitude,
    which are blank for ~8% of otherwise-valid stops. The CSV is downloaded automatically (cached unless
    force_download).
    """
    csv_path = _download_naptan(force_download=ctx.force_download)

    # SQL CASE mapping StopType -> stop_category, built from STOP_CATEGORY.
    category_case = "\n".join(f"                    WHEN '{code}' THEN '{cat}'" for code, cat in STOP_CATEGORY.items())

    con = duckdb_connector(writeable=True)
    try:
        print(f"  Parsing NAPTAN export {csv_path}…")
        con.execute(f"""
            CREATE TABLE naptan AS
            SELECT
                * EXCLUDE pt,
                lower(hex(h3_latlng_to_cell(ST_Y(pt), ST_X(pt), 9))) AS h3_9_id
            FROM (
                SELECT *, ST_Transform(geom, 'EPSG:27700', 'EPSG:4326', always_xy := true) AS pt
                FROM (
                    SELECT
                        ATCOCode AS atco_code,
                        NaptanCode AS naptan_code,
                        CommonName AS name,
                        Street AS street,
                        Indicator AS indicator,
                        Bearing AS bearing,
                        LocalityName AS locality_name,
                        Town AS town,
                        StopType AS stop_type,
                        CASE StopType
{category_case}
                            ELSE 'other'
                        END AS stop_category,
                        ST_Point(CAST(Easting AS DOUBLE), CAST(Northing AS DOUBLE)) AS geom
                    FROM read_csv_auto('{csv_path}', all_varchar=true)
                    WHERE Status = 'active'
                      AND TRY_CAST(Easting AS DOUBLE) > 0
                      AND TRY_CAST(Northing AS DOUBLE) > 0
                )
            );
        """)
        row_count = con.execute("SELECT COUNT(*) FROM naptan").fetchone()[0]  # ty:ignore[not-subscriptable]
        write_geoparquet(con, "SELECT * FROM naptan", ctx.parquet("naptan"))
    finally:
        con.close()
    print(f"  naptan: {row_count:,} rows")


DATASET = Dataset(name="naptan", table="naptan", extract=extract)
