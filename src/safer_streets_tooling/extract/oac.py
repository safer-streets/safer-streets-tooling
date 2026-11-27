"""2021 Output Area Classification → ``oac``, ``oac_classification``, ``oac_features`` and ``oac_centroids``.

Four attribute-only (no geometry) tables, split so the subgroup names are held once rather than
repeated across every OA:

- ``oac`` — one row per OA: ``spatial_id`` (the OA21 code) + its OAC ``code``. The code is the full
  hierarchical subgroup code (``1a1`` → group ``1a`` → supergroup ``1``), so the group and supergroup are
  prefixes of it. A consumer joins it via ``h3r*_geogs.oa21cd``.
- ``oac_classification`` — the decode/dimension table, one row per ``code`` (~51): the subgroup, group
  and supergroup *names* for that code. The group / supergroup codes aren't stored (they're prefixes of
  ``code``). Joined on ``oac.code``.
- ``oac_features`` — one row per E&W OA: ``spatial_id`` + the 60 classification inputs ``v01``–``v60``
  (asinh + range-standardised, so each is in [0, 1]).
- ``oac_centroids`` — the k-means centroid of every class at every level, one row per ``code``
  (``1`` … ``8``, ``1a`` …, ``1a1`` …) + ``v01``–``v60``, in the same space as ``oac_features``.

Like ``retail_centres`` / ``land_cover`` the labels need a login (GeoDS), so the two CSVs
(``oac21ew.csv`` per-OA codes, ``classification_codes_and_names-1.csv`` the code→name lookup) are
downloaded manually into the data directory's ``raw`` folder. Both have stray surrounding whitespace in
some codes/names, which is trimmed.

The features and centroids are open and downloaded from the safer-streets fork of the final OAC
repository (pinned in the catalogue). The published E&W labels are the interim Preview OAC's, copied
unchanged into the final OAC, so both come from the Preview: the centroids are the Preview's, vendored
unchanged in the fork from its repository (jakubwyszomierski/OAC2021-2), and the fork's E&W feature rows
were replaced with the Preview inputs — the final repo's own E&W inputs were recalculated after the
clustering and don't reproduce the labels (safer-streets/Output_Area_Classification#1). Scotland/NI rows
are dropped: they lack two of the variables, and no OA there is in ``output_areas_2021``.
"""

import re
from pathlib import Path

import duckdb
from safer_streets_core.database import duckdb_connector, write_geoparquet

from safer_streets_tooling.config import data_source
from safer_streets_tooling.extract._common import download, raw_dir
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


def _download(key: str, *, force_download: bool) -> Path:
    """The catalogue's ``key`` URL, cached in the raw folder under its file name."""
    url = data_source("oac")[key]
    path = raw_dir() / url.rsplit("/", 1)[-1]
    if not path.exists() or force_download:
        download(url, path)
    return path


def _variables(con: duckdb.DuckDBPyConnection, source: str) -> list[str]:
    """The ``vNN`` columns of ``source``, in numeric order (the source files list them out of order)."""
    columns = [d[0] for d in con.execute(f"SELECT * FROM {source} LIMIT 0").description]
    return sorted(c for c in columns if re.fullmatch(r"v\d\d", c))


def extract_oac_features(ctx: ExtractContext) -> None:
    """Write the ``oac_features`` parquet: each E&W OA's 60 classification inputs."""
    path = _download("features_url", force_download=ctx.force_download)
    print(f"  Loading oac_features from {path}…")
    con = duckdb_connector(writeable=True)
    try:
        source = f"read_parquet('{path}')"
        variables = ", ".join(_variables(con, source))
        con.execute(f"""
            CREATE TABLE oac_features AS
            SELECT OA AS spatial_id, {variables}
            FROM {source}
            WHERE OA[1] IN ('E', 'W')
            ORDER BY spatial_id;
        """)
        row_count = con.execute("SELECT COUNT(*) FROM oac_features").fetchone()[0]  # ty:ignore[not-subscriptable]
        write_geoparquet(con, "SELECT * FROM oac_features", ctx.parquet("oac_features"))
    finally:
        con.close()
    print(f"  oac_features: {row_count:,} rows")


def extract_oac_centroids(ctx: ExtractContext) -> None:
    """Write the ``oac_centroids`` parquet: the supergroup, group and subgroup centroids, one row per code."""
    sources = {
        id_col: f"read_csv('{_download(key, force_download=ctx.force_download)}')"
        for id_col, key in (
            ("Supergroup8", "supergroup_centroids_url"),
            ("Group", "group_centroids_url"),
            ("Subgroup", "subgroup_centroids_url"),
        )
    }
    con = duckdb_connector(writeable=True)
    try:
        variables = ", ".join(f"{v}::DOUBLE AS {v}" for v in _variables(con, sources["Subgroup"]))
        union = " UNION ALL ".join(
            f'SELECT "{id_col}"::VARCHAR AS code, {variables} FROM {source}' for id_col, source in sources.items()
        )
        con.execute(f"CREATE TABLE oac_centroids AS {union} ORDER BY code;")
        row_count = con.execute("SELECT COUNT(*) FROM oac_centroids").fetchone()[0]  # ty:ignore[not-subscriptable]
        write_geoparquet(con, "SELECT * FROM oac_centroids", ctx.parquet("oac_centroids"))
    finally:
        con.close()
    print(f"  oac_centroids: {row_count:,} rows")


DATASETS: tuple[Dataset, ...] = (
    Dataset(
        name="oac",
        table="oac",
        extract=extract_oac,
        description="2021 Output Area Classification code per OA (CDRC), keyed by oa21cd.",
        geometry=False,
    ),
    Dataset(
        name="oac_classification",
        table="oac_classification",
        extract=extract_oac_classification,
        description="OAC dimension table decoding each code to its supergroup/group/subgroup names.",
        geometry=False,
    ),
    Dataset(
        name="oac_features",
        table="oac_features",
        extract=extract_oac_features,
        description="OAC classification inputs v01–v60 (asinh + range-standardised) per E&W OA, keyed by oa21cd.",
        geometry=False,
    ),
    Dataset(
        name="oac_centroids",
        table="oac_centroids",
        extract=extract_oac_centroids,
        description="OAC k-means centroids (v01–v60) for every supergroup, group and subgroup, keyed by code.",
        geometry=False,
    ),
)
