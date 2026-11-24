"""Tests for the OAC pen-portraits and embeddings extracts: parsing the PDF's text, one row per subgroup, and
embedding it with a stand-in Gemini client (no network or API key)."""

from pathlib import Path
from types import SimpleNamespace
from typing import cast

import duckdb
import numpy as np
import pandas as pd
import pytest
from google import genai

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


class _FakeModels:
    """Stands in for ``genai.Client().models``: one vector per text, recording each request."""

    def __init__(self, dims: int = oac.EMBEDDING_DIM) -> None:
        self.dims = dims
        self.calls: list[dict] = []

    def embed_content(self, *, model: str, contents: list[str], config: object) -> SimpleNamespace:
        self.calls.append({"model": model, "contents": contents, "config": config})
        return SimpleNamespace(
            embeddings=[SimpleNamespace(values=[float(i + 1)] + [1.0] * (self.dims - 1)) for i in range(len(contents))]
        )


def _fake_client(models: _FakeModels) -> genai.Client:
    return cast(genai.Client, SimpleNamespace(models=models))


def test_embedding_text_puts_names_over_descriptions() -> None:
    row = pd.Series(
        {
            "supergroup_name": "Retired Professionals",
            "supergroup_description": "S.",
            "group_name": "Spacious Rural Living",
            "group_description": "G.",
            "subgroup_name": "Pre-Retirement Spacious Living",
            "subgroup_description": "SG.",
        }
    )
    assert oac.embedding_text(row) == (
        "# Retired Professionals\n\nS.\n\n## Spacious Rural Living\n\nG.\n\n### Pre-Retirement Spacious Living\n\nSG."
    )


def test_embed_returns_unit_vectors_in_one_request() -> None:
    models = _FakeModels()
    vectors = oac.embed(["a", "b", "c"], _fake_client(models))

    assert vectors.shape == (3, oac.EMBEDDING_DIM)
    assert vectors.dtype == np.float32
    np.testing.assert_allclose(np.linalg.norm(vectors, axis=1), 1.0, rtol=1e-6)
    assert len(models.calls) == 1
    call = models.calls[0]
    assert call["model"] == oac.EMBEDDING_MODEL
    assert call["config"].output_dimensionality == oac.EMBEDDING_DIM
    assert call["config"].task_type == oac.EMBEDDING_TASK


def test_embed_rejects_wrong_dimension() -> None:
    with pytest.raises(ValueError, match="expected 2 embeddings"):
        oac.embed(["a", "b"], _fake_client(_FakeModels(dims=3)))


def _stage_oac_inputs(ctx: ExtractContext, monkeypatch: pytest.MonkeyPatch, *, drop_name: str | None = None) -> None:
    """Write the two parquet ``oac_embeddings`` reads: pen portraits (from synthetic PDF text) and names."""
    pages = [SimpleNamespace(extract_text=lambda: _document(_codes()))]
    monkeypatch.setattr(oac, "_source_file", lambda key, label: ctx.staging / "pen_portraits.pdf")
    monkeypatch.setattr(oac, "PdfReader", lambda path: SimpleNamespace(pages=pages))
    oac.extract_oac_pen_portraits(ctx)
    subgroups = [c for c in _codes() if len(c) == 3 and c != drop_name]
    pd.DataFrame(
        {
            "code": subgroups,
            "subgroup_name": [f"Subgroup {c}" for c in subgroups],
            "group_name": [f"Group {c[:2]}" for c in subgroups],
            "supergroup_name": [f"Supergroup {c[0]}" for c in subgroups],
        }
    ).to_parquet(ctx.parquet("oac_classification"))


def test_extract_oac_embeddings_writes_parquet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ctx = ExtractContext(staging=tmp_path)
    _stage_oac_inputs(ctx, monkeypatch)
    models = _FakeModels()
    monkeypatch.setattr(oac, "load_dotenv", lambda: None)
    monkeypatch.setenv("GEMINI_API_KEY", "test-key")
    monkeypatch.setattr(oac.genai, "Client", lambda: _fake_client(models))

    oac.extract_oac_embeddings(ctx)

    out = ctx.parquet("oac_embeddings")
    schema = dict(duckdb.sql(f"DESCRIBE SELECT * FROM '{out}'").fetchall()[i][:2] for i in range(3))
    # parquet has no fixed-size list, so the FLOAT[768] written reads back as FLOAT[]
    assert schema == {"code": "VARCHAR", "text": "VARCHAR", "embedding": "FLOAT[]"}
    # in double: DuckDB sums a FLOAT[] dot product in float32, ~1e-5 off over 768 terms
    norm2 = "list_dot_product(embedding::DOUBLE[], embedding::DOUBLE[])"
    rows = duckdb.sql(f"SELECT code, text, {norm2}, len(embedding) FROM '{out}'")
    rows = sorted(rows.fetchall())
    assert len(rows) == 52
    assert {n for *_, n in rows} == {oac.EMBEDDING_DIM}
    rows = [r[:3] for r in rows]
    assert rows[0][1].startswith("# Supergroup 1\n\n")
    assert "### Subgroup 1a1" in rows[0][1]
    assert all(abs(norm2 - 1.0) < 1e-5 for _, _, norm2 in rows)
    assert models.calls[0]["contents"] == [text for _, text, _ in rows]


def test_extract_oac_embeddings_without_api_key(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(oac, "load_dotenv", lambda: None)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="GEMINI_API_KEY"):
        oac.extract_oac_embeddings(ExtractContext(staging=tmp_path))


def test_extract_oac_embeddings_rejects_unnamed_code(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ctx = ExtractContext(staging=tmp_path)
    _stage_oac_inputs(ctx, monkeypatch, drop_name="7b2")
    monkeypatch.setattr(oac, "load_dotenv", lambda: None)
    monkeypatch.setenv("GEMINI_API_KEY", "test-key")
    monkeypatch.setattr(oac.genai, "Client", lambda: _fake_client(_FakeModels()))
    with pytest.raises(ValueError, match=r"\['7b2'\]"):
        oac.extract_oac_embeddings(ctx)
