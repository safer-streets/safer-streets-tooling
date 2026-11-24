"""2021 Output Area Classification (CDRC) → ``oac``, ``oac_classification``, ``oac_pen_portraits``, ``oac_embeddings``.

Four attribute-only (no geometry) tables, split so the subgroup names, descriptions and embeddings are
held once rather than repeated across every OA:

- ``oac`` — one row per OA: ``spatial_id`` (the OA21 code) + its OAC ``code``. The code is the full
  hierarchical subgroup code (``1a1`` → group ``1a`` → supergroup ``1``), so the group and supergroup are
  prefixes of it. A consumer joins it via ``h3r*_geogs.oa21cd``.
- ``oac_classification`` — the decode/dimension table, one row per ``code`` (~51): the subgroup, group
  and supergroup *names* for that code. The group / supergroup codes aren't stored (they're prefixes of
  ``code``). Joined on ``oac.code``.
- ``oac_pen_portraits`` — one row per ``code`` (52): the published pen-portrait description of its
  supergroup, group and subgroup, parsed from the GeoDS pen-portraits PDF. Names are left to
  ``oac_classification`` (the PDF's headings don't all match it, e.g. 1b1). Joined on ``oac.code``.
- ``oac_embeddings`` — one row per ``code`` (52): a markdown ``text`` of the code's names over their pen
  portraits, and its Gemini ``embedding`` (unit length, so cosine similarity is a dot product). Needs a
  ``GEMINI_API_KEY``. Joined on ``oac.code``.

Like ``retail_centres`` / ``land_cover`` the source needs a login (GeoDS), so the two CSVs
(``oac21ew.csv`` per-OA codes, ``classification_codes_and_names-1.csv`` the code→name lookup) and the
pen-portraits PDF are downloaded manually into the data directory's ``raw`` folder. Both CSVs have stray
surrounding whitespace in some codes/names, which is trimmed.
"""

import logging
import os
import re
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from google import genai
from google.genai import types
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


EMBEDDING_MODEL = "gemini-embedding-001"
EMBEDDING_DIM = 768
EMBEDDING_TASK = "RETRIEVAL_DOCUMENT"


def embedding_text(row: pd.Series) -> str:
    """The markdown document embedded for one subgroup: each level's name as a heading over its pen portrait."""
    return (
        f"# {row.supergroup_name}\n\n{row.supergroup_description}\n\n"
        f"## {row.group_name}\n\n{row.group_description}\n\n"
        f"### {row.subgroup_name}\n\n{row.subgroup_description}"
    )


def embed(texts: list[str], client: genai.Client) -> np.ndarray:
    """Embed ``texts`` in one request (the API takes up to 100), returning unit-length float32 rows."""
    contents: list[types.ContentUnion] = [*texts]  # list is invariant, so list[str] isn't accepted as-is
    result = client.models.embed_content(
        model=EMBEDDING_MODEL,
        contents=contents,
        config=types.EmbedContentConfig(output_dimensionality=EMBEDDING_DIM, task_type=EMBEDDING_TASK),
    )
    vectors = np.array([e.values for e in result.embeddings or []], dtype=np.float32)
    if vectors.shape != (len(texts), EMBEDDING_DIM):
        raise ValueError(f"expected {len(texts)} embeddings of {EMBEDDING_DIM} dims, got shape {vectors.shape}")
    # only the full 3072-d output comes normalised; scaling here makes cosine similarity a dot product
    return vectors / np.linalg.norm(vectors, axis=1, keepdims=True)


def extract_oac_embeddings(ctx: ExtractContext) -> None:
    """Write the ``oac_embeddings`` parquet: per subgroup ``code``, the embedded text and its Gemini embedding.

    Reads the names from ``oac_classification`` and the descriptions from ``oac_pen_portraits``. Needs
    ``GEMINI_API_KEY`` in the environment or a ``.env`` file; without it the dataset is skipped.
    """
    load_dotenv()
    if not (os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")):
        raise RuntimeError("GEMINI_API_KEY is not set (environment or .env), so OAC embeddings can't be generated")
    names_pq, portraits_pq = ctx.parquet("oac_classification"), ctx.parquet("oac_pen_portraits")
    print(f"  Embedding oac_pen_portraits with {EMBEDDING_MODEL}…")
    con = duckdb_connector(writeable=True)
    try:
        rows = con.execute(f"""
            SELECT p.*, c.supergroup_name, c.group_name, c.subgroup_name
            FROM read_parquet('{portraits_pq}') p
            LEFT JOIN read_parquet('{names_pq}') c USING (code)
            ORDER BY code
        """).df()
        unnamed = rows.loc[rows.subgroup_name.isna(), "code"].tolist()
        if unnamed:
            raise ValueError(f"OAC codes with pen portraits but no oac_classification names: {unnamed}")
        table = pd.DataFrame({"code": rows.code, "text": rows.apply(embedding_text, axis=1)})
        table["embedding"] = list(embed(table.text.tolist(), genai.Client()))
        con.register("oac_embeddings_stg", table)
        # parquet stores it as a plain list (FLOAT[] on read); the fixed-size cast fails on a wrong length
        write_geoparquet(
            con,
            f"SELECT code, text, embedding::FLOAT[{EMBEDDING_DIM}] AS embedding FROM oac_embeddings_stg",
            ctx.parquet("oac_embeddings"),
        )
    finally:
        con.close()
    print(f"  oac_embeddings: {len(table):,} rows")


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
    Dataset(
        name="oac_embeddings",
        table="oac_embeddings",
        extract=extract_oac_embeddings,
        description=(
            f"Unit-length {EMBEDDING_DIM}-d {EMBEDDING_MODEL} ({EMBEDDING_TASK}) embedding of each OAC code's "
            "names and pen portraits."
        ),
        geometry=False,
        depends_on=("oac_classification", "oac_pen_portraits"),
    ),
)
