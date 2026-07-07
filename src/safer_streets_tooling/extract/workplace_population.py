"""Census 2021 WP001 workplace population per output area → ``workplace_population.parquet``.

Attribute-only (no geometry). The workplace population is an estimate of the usually resident
population aged 16 years and over, working in an area. It includes people who work mainly at or from
home, or do not have a fixed place of work, in their area of usual residence.

The source is the nomis Census 2021 workplace population bulk download
(https://www.nomisweb.co.uk/sources/census_2021_wp): ``wp001.zip`` holds one CSV per geography level,
of which only the OA-level ``WP001_oa.csv`` is used — one row per 2021 output area, keyed by
``spatial_id`` (the OA21 code, joinable to ``buildings.oa21cd`` / ``h3_*_geogs.oa21cd``).
"""

from safer_streets_core.database import duckdb_connector, write_geoparquet

from safer_streets_tooling.config import data_source
from safer_streets_tooling.extract._common import download, extract_cached, raw_dir
from safer_streets_tooling.extract.base import Dataset, ExtractContext


def extract(ctx: ExtractContext) -> None:
    """Write the ``workplace_population`` parquet: one row per OA — ``spatial_id`` (= ``oa21cd``) +
    its ``workplace_population`` count.

    The bulk zip is downloaded from nomis and cached under the raw folder (reused unless
    force_download); the OA-level member CSV is extracted beside it.
    """
    src = data_source("workplace_population")
    zip_path = raw_dir() / src["zip"]
    if ctx.force_download or not zip_path.exists():
        download(src["url"], zip_path)
    else:
        print(f"  Using cached {zip_path}")
    csv_path = extract_cached(zip_path, src["member"])

    print(f"  Loading workplace_population from {csv_path}…")
    con = duckdb_connector(writeable=True)
    try:
        con.execute(f"""
            CREATE TABLE workplace_population AS
            SELECT "Output Areas Code" AS spatial_id, "Count" AS workplace_population
            FROM read_csv('{csv_path}');
        """)
        row_count, total = con.execute(
            "SELECT COUNT(*), SUM(workplace_population) FROM workplace_population"
        ).fetchone()  # ty:ignore[not-iterable]
        write_geoparquet(con, "SELECT * FROM workplace_population", ctx.parquet("workplace_population"))
    finally:
        con.close()
    print(f"  workplace_population: {row_count:,} rows ({total:,} people)")


DATASET = Dataset(
    name="workplace_population",
    table="workplace_population",
    extract=extract,
    description="Census 2021 WP001 workplace population (residents 16+ working in the area) per OA, keyed by oa21cd.",
    geometry=False,
)
