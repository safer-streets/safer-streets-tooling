"""Tests for the OAC pen-portraits extract: parsing the PDF's text and writing one row per subgroup."""

from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest

from safer_streets_tooling.extract import oac
from safer_streets_tooling.extract.base import ExtractContext

# The real hierarchy's shape: supergroup → {group letter: number of subgroups} (8 / 21 / 52).
_SHAPE = {
    "1": {"a": 2, "b": 2, "c": 2},
    "2": {"a": 3, "b": 2, "c": 2},
    "3": {"a": 4, "b": 2, "c": 2},
    "4": {"a": 3, "b": 4, "c": 2},
    "5": {"a": 3, "b": 2},
    "6": {"a": 3, "b": 3, "c": 2},
    "7": {"a": 2, "b": 2},
    "8": {"a": 2, "b": 3},
}
# Heading spellings the PDF uses besides "<code>: Name".
_HEADINGS = {"2": "2:  Name", "3c1": "3c1: – Name", "4b4": "4b4 - Name", "5a1": "5a1 – Name", "6c1": "6c1 Name"}
# A body spread over lines as pypdf extracts it: a word hyphenated across a line break, a page number, a
# line starting with digits that is not a heading, and a typo that is in the published PDF.
_MULTILINE = {"1a1": "Most live in single-\nperson households\n13\n45 or over, andmany\nown two cars.."}


def _codes() -> list[str]:
    codes = []
    for sg, groups in _SHAPE.items():
        codes.append(sg)
        for letter, n in groups.items():
            codes.append(sg + letter)
            codes.extend(f"{sg}{letter}{i}" for i in range(1, n + 1))
    return codes


def _document(codes: list[str]) -> str:
    sections = [f"{_HEADINGS.get(c, f'{c}: Name')}\n{_MULTILINE.get(c, f'Portrait of {c}.')}" for c in codes]
    return "\n".join(
        [
            "Contents\nPen Portraits\nUpdate Technical Note",
            "Pen Portraits",
            oac._PORTRAITS_START,
            *sections,
            "",
            "31",
            oac._PORTRAITS_END,
            "1) Calculate the cluster mean values",
        ]
    )


def test_parse_pen_portraits_reads_every_level() -> None:
    portraits = oac.parse_pen_portraits(_document(_codes()))
    assert len(portraits) == 81
    assert portraits["1"] == "Portrait of 1."
    assert portraits["4b"] == "Portrait of 4b."
    assert portraits["8b3"] == "Portrait of 8b3."


@pytest.mark.parametrize("code", sorted(_HEADINGS))
def test_parse_pen_portraits_heading_variants(code: str) -> None:
    assert oac.parse_pen_portraits(_document(_codes()))[code] == f"Portrait of {code}."


def test_parse_pen_portraits_cleans_body() -> None:
    portraits = oac.parse_pen_portraits(_document(_codes()))
    assert portraits["1a1"] == "Most live in single-person households 45 or over, and many own two cars."


def test_parse_pen_portraits_stops_at_technical_note() -> None:
    assert "cluster mean" not in oac.parse_pen_portraits(_document(_codes()))["8b3"]


def test_parse_pen_portraits_rejects_a_missing_heading() -> None:
    codes = [c for c in _codes() if c != "7b2"]
    with pytest.raises(ValueError, match="8/21/52"):
        oac.parse_pen_portraits(_document(codes))


def test_pen_portrait_table_has_one_row_per_subgroup() -> None:
    table = oac.pen_portrait_table(oac.parse_pen_portraits(_document(_codes())))
    assert len(table) == 52
    assert list(table.columns) == ["code", "supergroup_description", "group_description", "subgroup_description"]
    row = table.set_index("code").loc["3c1"]
    assert (row.supergroup_description, row.group_description, row.subgroup_description) == (
        "Portrait of 3.",
        "Portrait of 3c.",
        "Portrait of 3c1.",
    )


def test_extract_oac_pen_portraits_writes_parquet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pages = [SimpleNamespace(extract_text=lambda: _document(_codes()))]
    monkeypatch.setattr(oac, "_source_file", lambda key, label: tmp_path / "pen_portraits.pdf")
    monkeypatch.setattr(oac, "PdfReader", lambda path: SimpleNamespace(pages=pages))

    ctx = ExtractContext(staging=tmp_path)
    oac.extract_oac_pen_portraits(ctx)

    rows = duckdb.sql(f"SELECT code, subgroup_description FROM '{ctx.parquet('oac_pen_portraits')}' ORDER BY code")
    assert rows.fetchall()[:2] == [
        ("1a1", oac.parse_pen_portraits(_document(_codes()))["1a1"]),
        ("1a2", "Portrait of 1a2."),
    ]


def test_extract_oac_pen_portraits_missing_pdf(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(oac, "raw_dir", lambda: tmp_path)
    with pytest.raises(FileNotFoundError, match="pen portraits PDF not found"):
        oac.extract_oac_pen_portraits(ExtractContext(staging=tmp_path))
