"""2021 Output Area Classification (CDRC) → ``oac``, ``oac_classification`` and ``oac_pen_portraits`` parquet.

Three attribute-only (no geometry) tables, split so the subgroup names and descriptions are held once
rather than repeated across every OA:

- ``oac`` — one row per OA: ``spatial_id`` (the OA21 code) + its OAC ``code``. The code is the full
  hierarchical subgroup code (``1a1`` → group ``1a`` → supergroup ``1``), so the group and supergroup are
  prefixes of it. A consumer joins it via ``h3r*_geogs.oa21cd``.
- ``oac_classification`` — the decode/dimension table, one row per ``code`` (~51): the subgroup, group
  and supergroup *names* for that code. The group / supergroup codes aren't stored (they're prefixes of
  ``code``). Joined on ``oac.code``.
- ``oac_pen_portraits`` — one row per ``code`` (52): the published pen-portrait description of its
  supergroup, group and subgroup, parsed from the GeoDS pen-portraits PDF. Names are left to
  ``oac_classification`` (the PDF's headings don't all match it, e.g. 1b1). Joined on ``oac.code``.

Like ``retail_centres`` / ``land_cover`` the source needs a login (GeoDS), so the two CSVs
(``oac21ew.csv`` per-OA codes, ``classification_codes_and_names-1.csv`` the code→name lookup) and the
pen-portraits PDF are downloaded manually into the data directory's ``raw`` folder. Both CSVs have stray
surrounding whitespace in some codes/names, which is trimmed.
"""

import logging
import re
from pathlib import Path

import pandas as pd
from pypdf import PdfReader
from safer_streets_core.database import duckdb_connector, write_geoparquet

from safer_streets_tooling.config import data_source
from safer_streets_tooling.extract._common import raw_dir
from safer_streets_tooling.extract.base import Dataset, ExtractContext


def _source_file(key: str, label: str) -> Path:
    """Resolve a manually-downloaded OAC source file, raising with download instructions if absent."""
    src = data_source("oac")
    path = raw_dir() / src[key]
    if not path.exists():
        raise FileNotFoundError(
            f"2021 OAC {label} not found: {path}\n"
            f"Download {src[key]} from GeoDS (https://data.geods.ac.uk/, login required) "
            f"and place it in the raw data directory."
        )
    return path


def extract_oac(ctx: ExtractContext) -> None:
    """Write the ``oac`` parquet: one row per OA — ``spatial_id`` (= ``oa21cd``) + its OAC ``code``.

    The full subgroup code is kept; the group and supergroup are prefixes of it, and the names at every
    tier are decoded via the ``oac_classification`` table.
    """
    oa_csv = _source_file("oa", "per-OA classification CSV")
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
    names_csv = _source_file("names", "codes-and-names CSV")
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


# Headings are mostly "1: Name", "1a: Name", "1a1: Name", but the PDF also has "3c1: – Name", "4b4 - Name",
# "5a1 – Name" and a bare "6c1 Name" (the last only at subgroup level).
_HEADING = re.compile(r"^(?P<code>[1-8](?:[a-d]\d?)?)(?:\s*[:\-–]\s*)+[A-Z]|^(?P<subgroup>[1-8][a-d]\d)\s+[A-Z]")
_PORTRAITS_START = "In what follows patterns described are relative to the UK average."
_PORTRAITS_END = "Update Technical Note"
# Typos in the published PDF itself, not extraction artefacts.
_PDF_TYPOS = {
    "andmany": "and many",
    "rentedflats": "rented flats",
    "relatiove": "relative",
    "typically nUK born": "typically UK-born",
}
_EXPECTED_COUNTS = {1: 8, 2: 21, 3: 52}


def parse_pen_portraits(text: str) -> dict[str, str]:
    """Map every OAC code (supergroup ``1``, group ``1a``, subgroup ``1a1``) to its pen-portrait text.

    ``text`` is the whole document's extracted text. Only the "Pen Portraits" section is read (it opens
    with a fixed sentence and runs to the technical note); the table of contents mentions the technical
    note too, so the end marker is searched for after the start. Page numbers are dropped, and a word
    hyphenated across a line break is rejoined keeping its hyphen ("single-" + "person").
    """
    start = text.index(_PORTRAITS_START) + len(_PORTRAITS_START)
    section = text[start : text.index(_PORTRAITS_END, start)]

    bodies: dict[str, list[str]] = {}
    current: str | None = None
    for raw in section.splitlines():
        line = raw.strip()
        if not line or line.isdigit():
            continue
        if m := _HEADING.match(line):
            current = m["code"] or m["subgroup"]
            bodies[current] = []
        elif current is not None:
            bodies[current].append(line)

    portraits = {}
    for code, lines in bodies.items():
        body = re.sub(r"(\w)-\n", r"\1-", "\n".join(lines))
        body = " ".join(body.split())
        for typo, fix in _PDF_TYPOS.items():
            body = body.replace(typo, fix)
        portraits[code] = re.sub(r"\.{2,}", ".", body)

    counts = pd.Series([len(code) for code in portraits]).value_counts().to_dict()
    if counts != _EXPECTED_COUNTS:
        raise ValueError(f"expected 8/21/52 OAC supergroup/group/subgroup pen portraits, parsed {counts}")
    return portraits


def pen_portrait_table(portraits: dict[str, str]) -> pd.DataFrame:
    """One row per subgroup ``code`` carrying the description at each of its three levels."""
    subgroups = sorted(code for code in portraits if len(code) == 3)
    return pd.DataFrame(
        {
            "code": subgroups,
            "supergroup_description": [portraits[code[0]] for code in subgroups],
            "group_description": [portraits[code[:2]] for code in subgroups],
            "subgroup_description": [portraits[code] for code in subgroups],
        }
    )


def extract_oac_pen_portraits(ctx: ExtractContext) -> None:
    """Write the ``oac_pen_portraits`` parquet: the pen-portrait descriptions for each subgroup ``code``."""
    pdf = _source_file("pen_portraits", "pen portraits PDF")
    print(f"  Loading oac_pen_portraits from {pdf}…")
    # the PDF has broken xref offsets that pypdf recovers from, warning once per object
    logging.getLogger("pypdf").setLevel(logging.ERROR)
    text = "\n".join(page.extract_text() for page in PdfReader(pdf).pages)
    table = pen_portrait_table(parse_pen_portraits(text))
    con = duckdb_connector(writeable=True)
    try:
        con.register("oac_pen_portraits_stg", table)
        write_geoparquet(con, "SELECT * FROM oac_pen_portraits_stg", ctx.parquet("oac_pen_portraits"))
    finally:
        con.close()
    print(f"  oac_pen_portraits: {len(table):,} rows")


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
        name="oac_pen_portraits",
        table="oac_pen_portraits",
        extract=extract_oac_pen_portraits,
        description="OAC pen-portrait descriptions (GeoDS) of each code's supergroup, group and subgroup.",
        geometry=False,
    ),
)
