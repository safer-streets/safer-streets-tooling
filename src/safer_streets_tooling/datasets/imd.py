"""English IoD + Welsh WIMD deprivation as per-LSOA percentiles → ``imd_scores_pct.parquet``.

Attribute-only (no geometry). The ``imd_scores_pct`` table covers all of England & Wales: English
LSOAs come from the English IoD (``data_source("imd")``), Welsh LSOAs from the WIMD
(``data_source("wimd")``). Both index the 2021 LSOA geography, so they share ``spatial_id`` and merge
by row. Percentiles are computed WITHIN each country (England vs Wales are separate deprivation indices
and not comparable across the border), so a percentile always means "relative to other LSOAs in the
same country".
"""

import duckdb
import pandas as pd
import requests
from safer_streets_core.database import duckdb_connector
from safer_streets_core.utils import data_dir

from safer_streets_tooling.config import data_source
from safer_streets_tooling.datasets._common import download, write_geoparquet
from safer_streets_tooling.datasets.base import Dataset, ExtractContext

# original IoD column name -> short name; everything except IMD_PASSTHROUGH is percentile-ranked. This
# is also the column set of the merged table, so the Welsh side maps onto the same short names.
IMD_COLUMNS = {
    "LSOA code (2021)": "spatial_id",
    "Local Authority District code (2024)": "lad24cd",
    "Local Authority District name (2024)": "lad24nm",
    "Index of Multiple Deprivation (IMD) Score": "imd_score",
    "Index of Multiple Deprivation (IMD) Rank (where 1 is most deprived)": "imd_rank",
    "Income Score (rate)": "income",
    "Employment Score (rate)": "employment",
    "Education, Skills and Training Score": "est",
    "Health Deprivation and Disability Score": "hdd",
    "Crime Score": "crime",
    "Barriers to Housing and Services Score": "bhs",
    "Living Environment Score": "le",
}
IMD_PASSTHROUGH = ("spatial_id", "lad24cd", "lad24nm", "imd_rank")

# WIMD scores use the same "higher = more deprived" convention as the English IoD. Welsh domains map
# onto the English short names below; "Access to Services" + "Housing" are averaged into `bhs`
# (England's single "Barriers to Housing and Services" domain), and the overall WIMD score also yields
# `imd_rank` (1 = most deprived). There is no Welsh equivalent of the English sub-domains, which is why
# they are dropped from IMD_COLUMNS above.
WIMD_DOMAINS = {
    "WIMD 2025": "imd_score",
    "Income": "income",
    "Employment": "employment",
    "Education": "est",
    "Health": "hdd",
    "Community Safety": "crime",
    "Physical Environment": "le",
}


def _imd_england(*, force_download: bool = False) -> pd.DataFrame:
    """English IoD "File 7" as per-LSOA percentiles (the IMD_COLUMNS short names).

    The CSV is downloaded from gov.uk and cached under the data directory (reused unless
    force_download). Columns of interest are renamed and every score column (all except
    IMD_PASSTHROUGH) is replaced by its percentile rank within England (0–1, higher = more deprived).
    """
    src = data_source("imd")
    matches = sorted(data_dir().glob(src["glob"]))
    if force_download or not matches:
        csv_path = data_dir() / src["csv"]
        download(src["url"], csv_path)
    else:
        csv_path = matches[-1]
        print(f"  Using cached {csv_path}")

    imd = pd.read_csv(csv_path)[list(IMD_COLUMNS)].rename(columns=IMD_COLUMNS)
    for column in imd.columns:
        if column not in IMD_PASSTHROUGH:
            imd[column] = imd[column].rank(pct=True)
    return imd


def _welsh_lad_codes(ctx: ExtractContext) -> dict[str, str]:
    """Map LA name -> LAD24 code from the boundary parquet, so Welsh rows can be given an ``lad24cd``
    (the WIMD file carries the LA name but not its code). Empty if the boundary parquet isn't present
    or the lookup fails, in which case ``lad24cd`` is left null for Welsh rows."""
    lad_pq = ctx.parquet("local_authority_districts")
    if not lad_pq.exists():
        return {}
    con = duckdb_connector()
    try:
        return {
            name: code
            for code, name in con.execute(f"SELECT spatial_id, lad24nm FROM read_parquet('{lad_pq}')").fetchall()
        }
    except duckdb.Error:
        return {}
    finally:
        con.close()


def _imd_wales(ctx: ExtractContext, *, force_download: bool = False) -> pd.DataFrame:
    """Welsh WIMD scores as per-LSOA percentiles, on the same IMD_COLUMNS short names as England.

    The ODS is downloaded from gov.wales and cached under the data directory (reused unless
    force_download). WIMD scores use the same "higher = more deprived" convention as the English IoD,
    so each is percentile-ranked within Wales the same way. ``imd_rank`` is derived from the overall
    score (1 = most deprived); ``lad24cd`` is looked up from the boundary parquet by LA name.
    """
    src = data_source("wimd")
    ods_path = data_dir() / src["ods"]
    if force_download or not ods_path.exists():
        download(src["url"], ods_path)
    else:
        print(f"  Using cached {ods_path}")

    raw = pd.read_excel(ods_path, engine="odf", sheet_name=src["sheet"], header=src["header_row"])
    raw = raw.rename(columns=lambda c: str(c).strip())

    wales = pd.DataFrame({"spatial_id": raw["LSOA code"], "lad24nm": raw["Local Authority name"]})
    wales["lad24cd"] = wales["lad24nm"].map(_welsh_lad_codes(ctx))
    wales["imd_rank"] = raw["WIMD 2025"].rank(ascending=False, method="min").astype("int64")
    for ods_column, short in WIMD_DOMAINS.items():
        wales[short] = raw[ods_column].rank(pct=True)
    # England's single "Barriers to Housing and Services" domain ≈ Wales' "Access to Services" +
    # "Housing"; average the two scores, then percentile-rank that composite.
    wales["bhs"] = ((raw["Access to Services"] + raw["Housing"]) / 2).rank(pct=True)
    return wales[list(IMD_COLUMNS.values())]


def extract(ctx: ExtractContext) -> None:
    """
    Write the ``imd_scores_pct`` parquet: England (IoD) + Wales (WIMD) deprivation as per-LSOA percentiles.

    Both indices are reduced to the same IMD_COLUMNS short names, percentile-ranked WITHIN each country
    (higher = more deprived), then unioned into one table keyed by ``spatial_id`` (the LSOA21 code) for
    joining to the lsoa geography. If the Welsh data can't be fetched the table is still built from
    England alone.
    """
    england = _imd_england(force_download=ctx.force_download)
    try:
        wales = _imd_wales(ctx, force_download=ctx.force_download)
    except (requests.RequestException, KeyError, ValueError) as exc:
        print(f"  Welsh WIMD unavailable ({exc}); building imd_scores_pct from England only")
        wales = england.iloc[:0]
    imd = pd.concat([england, wales], ignore_index=True)

    con = duckdb_connector(writeable=True)
    try:
        con.register("imd_scores_pct_stg", imd)
        write_geoparquet(con, "SELECT * FROM imd_scores_pct_stg", ctx.parquet("imd_scores_pct"))
    finally:
        con.unregister("imd_scores_pct_stg")
        con.close()
    print(f"  imd_scores_pct: {len(imd):,} rows")


DATASET = Dataset(
    name="imd_scores_pct",
    table="imd_scores_pct",
    extract=extract,
    geometry=False,
    depends_on=("local_authority_districts",),
)
