"""2021 Output Area Classification (CDRC) → ``oac.parquet`` + ``oac_classification.parquet``.

Two attribute-only (no geometry) tables, split so the subgroup names are held once rather than
repeated across every OA:

- ``oac`` — one row per OA: ``spatial_id`` (the OA21 code) + its OAC ``code``. The code is the full
  hierarchical subgroup code (``1a1`` → group ``1a`` → supergroup ``1``), so the group and supergroup are
  prefixes of it. A consumer joins it via ``h3_*_geogs.oa21cd``.
- ``oac_classification`` — the decode/dimension table, one row per ``code`` (~51): the subgroup, group
  and supergroup *names* for that code. The group / supergroup codes aren't stored (they're prefixes of
  ``code``). Joined on ``oac.code``.

Like ``retail_centres`` / ``land_cover`` the source needs a login (GeoDS), so the two CSVs
(``oac21ew.csv`` per-OA codes, ``classification_codes_and_names-1.csv`` the code→name lookup) are
downloaded manually into the data directory's ``raw`` folder. Both have stray surrounding whitespace in
some codes/names, which is trimmed.
"""

from pathlib import Path

from safer_streets_core.database import duckdb_connector, write_geoparquet

from safer_streets_tooling.config import data_source
from safer_streets_tooling.extract._common import raw_dir
from safer_streets_tooling.extract.base import Dataset, ExtractContext


def _source_csv(key: str, label: str) -> Path:
    """Resolve a manually-downloaded OAC source CSV, raising with download instructions if absent."""
    src = data_source("oac")
    path = raw_dir() / src[key]
    if not path.exists():
        raise FileNotFoundError(
            f"2021 OAC {label} CSV not found: {path}\n"
            f"Download {src[key]} from GeoDS (https://data.geods.ac.uk/, login required) "
            f"and place it in the raw data directory."
        )
    return path


def extract_oac(ctx: ExtractContext) -> None:
    """Write the ``oac`` parquet: one row per OA — ``spatial_id`` (= ``oa21cd``) + its OAC ``code``.

    The full subgroup code is kept; the group and supergroup are prefixes of it, and the names at every
    tier are decoded via the ``oac_classification`` table.
    """
    oa_csv = _source_csv("oa", "per-OA classification")
    print(f"  Loading oac from {oa_csv}…")
    con = duckdb_connector(writeable=True)
    try:
        con.execute(f"""
            CREATE TABLE oac AS
            SELECT oa21cd AS spatial_id, TRIM(subgroup) AS code
            FROM read_csv('{oa_csv}');
        """)
        row_count = con.execute("SELECT COUNT(*) FROM oac").fetchone()[0]  # ty:ignore[not-subscriptable]
        write_geoparquet(con, "SELECT * FROM oac", ctx.parquet("oac"))
    finally:
        con.close()
    print(f"  oac: {row_count:,} rows")


def extract_oac_classification(ctx: ExtractContext) -> None:
    """Write the ``oac_classification`` parquet: one row per ``code`` decoding the OAC hierarchy.

    Each subgroup row from the codes-and-names lookup is joined to its group (the code's first two
    characters) and supergroup (first character), so the table carries the subgroup / group / supergroup
    *names* for each ``code`` — the dimension table ``oac.code`` joins to.
    """
    names_csv = _source_csv("names", "codes-and-names")
    print(f"  Loading oac_classification from {names_csv}…")
    con = duckdb_connector(writeable=True)
    try:
        con.execute(f"""
            CREATE TABLE oac_classification AS
            WITH names AS (
                SELECT
                    "Level Code" AS level_code,
                    TRIM("Classification Code") AS code,
                    TRIM("Classification Name") AS name
                FROM read_csv('{names_csv}')
            )
            SELECT
                subg.code AS code,
                subg.name AS subgroup_name,
                g.name AS group_name,
                sg.name AS supergroup_name
            FROM names subg
            LEFT JOIN names g ON g.level_code = 'g' AND g.code = LEFT(subg.code, 2)
            LEFT JOIN names sg ON sg.level_code = 'sg' AND sg.code = LEFT(subg.code, 1)
            WHERE subg.level_code = 'subg'
            ORDER BY subg.code;
        """)
        row_count = con.execute("SELECT COUNT(*) FROM oac_classification").fetchone()[0]  # ty:ignore[not-subscriptable]
        write_geoparquet(con, "SELECT * FROM oac_classification", ctx.parquet("oac_classification"))
    finally:
        con.close()
    print(f"  oac_classification: {row_count:,} rows")


DATASETS: tuple[Dataset, ...] = (
    Dataset(name="oac", table="oac", extract=extract_oac, geometry=False),
    Dataset(name="oac_classification", table="oac_classification", extract=extract_oac_classification, geometry=False),
)
