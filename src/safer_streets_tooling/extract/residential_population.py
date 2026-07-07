"""Census 2021 TS001 usual residents per output area → ``residential_population.parquet``.

Attribute-only (no geometry). A usual resident is anyone who, on Census Day (21 March 2021), was in
the UK and had stayed or intended to stay in the UK for a period of 12 months or more, or had a
permanent UK address and was outside the UK and intended to be outside the UK for less than 12 months.

The source is the nomis API (table TS001 = ``NM_2021_1``; ported from the safer-streets-eda
``buildings.ipynb`` prototype): the count of usual residents per 2021 output area split by residence
type — ``household_population`` (lives in a household) and ``communal_population`` (lives in a communal
establishment, e.g. care homes, student halls, prisons) — keyed by ``spatial_id`` (the OA21 code,
joinable to ``buildings.oa21cd`` / ``h3_*_geogs.oa21cd``). Requires ``NOMIS_API_KEY`` (free
registration at https://www.nomisweb.co.uk); the dataset is optional, so the extract is skipped with a
warning when the key is absent.
"""

from safer_streets_core.database import duckdb_connector, write_geoparquet
from safer_streets_core.nomisweb import ALL_OAS_EW, BASE_URL, api_key

from safer_streets_tooling.config import data_source
from safer_streets_tooling.extract._common import download, raw_dir
from safer_streets_tooling.extract.base import Dataset, ExtractContext

# C2021_RESTYPE_3 residence-type code -> output column. Code 0 (the total) is derivable as their sum,
# so it is not fetched.
RESTYPE_COLUMNS = {1: "household_population", 2: "communal_population"}

# 2021 output areas in England & Wales; fewer rows per restype means nomis truncated the response
# (it caps unauthenticated / over-quota requests) and the parquet must not be written.
EXPECTED_OA_COUNT = 188_880


def extract(ctx: ExtractContext) -> None:
    """Write the ``residential_population`` parquet: one row per OA — ``spatial_id`` (= ``oa21cd``) +
    the usual-resident count split into ``household_population`` / ``communal_population``.

    The per-OA CSV is fetched from the nomis API (cached under the raw folder, reused unless
    force_download) and pivoted from one row per (OA, residence type) to one row per OA. ``measures``
    20100 is the count (not percentage). ``select`` is deliberately not used — nomis rejects it on this
    table — so the full column set is fetched and reduced here.
    """
    src = data_source("residential_population")
    try:
        key = api_key()
    except KeyError as e:
        raise RuntimeError(
            "NOMIS_API_KEY not set; register (free) at https://www.nomisweb.co.uk and set it in the "
            "environment or a .env file to extract residential_population."
        ) from e

    csv_path = raw_dir() / src["csv"]
    if ctx.force_download or not csv_path.exists():
        restypes = ",".join(str(code) for code in RESTYPE_COLUMNS)
        params = {"date": "latest", "geography": ALL_OAS_EW, "c2021_restype_3": restypes, "measures": "20100"} | key
        url = f"{BASE_URL}/dataset/{src['nm_table']}.data.csv?{'&'.join(f'{k}={v}' for k, v in params.items())}"
        download(url, csv_path)
    else:
        print(f"  Using cached {csv_path}")

    # the CAST keeps the counts integral (SUM widens to HUGEINT, which lands as float in parquet consumers)
    restype_cols = ",\n".join(
        f"CAST(SUM(OBS_VALUE) FILTER (WHERE C2021_RESTYPE_3 = {code}) AS BIGINT) AS {column}"
        for code, column in RESTYPE_COLUMNS.items()
    )
    print(f"  Loading residential_population from {csv_path}…")
    con = duckdb_connector(writeable=True)
    try:
        con.execute(f"""
            CREATE TABLE residential_population AS
            SELECT GEOGRAPHY_CODE AS spatial_id, {restype_cols}
            FROM read_csv('{csv_path}')
            GROUP BY GEOGRAPHY_CODE;
        """)
        row_count, total = con.execute(
            "SELECT COUNT(*), SUM(household_population + communal_population) FROM residential_population"
        ).fetchone()  # ty:ignore[not-iterable]
        if row_count != EXPECTED_OA_COUNT:
            raise RuntimeError(
                f"residential_population has {row_count:,} OAs, expected {EXPECTED_OA_COUNT:,} — the nomis "
                f"response was likely truncated (check NOMIS_API_KEY); delete {csv_path} and retry."
            )
        write_geoparquet(con, "SELECT * FROM residential_population", ctx.parquet("residential_population"))
    finally:
        con.close()
    print(f"  residential_population: {row_count:,} rows ({total:,} people)")


DATASET = Dataset(
    name="residential_population",
    table="residential_population",
    extract=extract,
    description="Census 2021 TS001 usual residents per OA (household + communal establishment), keyed by oa21cd.",
    geometry=False,
)
