"""Build ``index.parquet`` — a catalogue describing every extract + transform table.

The index is a small, geometry-free parquet with one row per ``*.parquet`` present under the extract and
transform directories. Each row carries the table's ``phase`` (``extract`` / ``transform``), ``name``,
a one-line ``description``, its ``n_rows`` / ``n_columns`` / ``columns`` schema summary, whether it
carries geometry, and ``last_modified`` — the parquet's mtime (UTC), i.e. when the table was last built
(``sync`` preserves it across machines) — so a consumer can discover the build outputs without opening
every file.

Descriptions are the single source of truth on the registries: ``Dataset.description`` for extract
tables and ``TransformStep.description`` for the relations a transform step emits. Keep those fields
current when a table changes and this catalogue follows automatically (see AGENTS.md).
"""

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd

from safer_streets_tooling.extract import DATASETS
from safer_streets_tooling.transform import STEPS
from safer_streets_tooling.transform.base import H3_RESOLUTIONS

INDEX_NAME = "index"

# Column order of the emitted index.parquet.
_COLUMNS = ["phase", "name", "description", "n_rows", "n_columns", "has_geometry", "columns", "last_modified"]


def _descriptions(con: duckdb.DuckDBPyConnection, resolutions: list[int]) -> dict[str, str]:
    """Map each table name to its registry description.

    Extract tables come straight from ``Dataset.description``; transform tables from the
    ``TransformStep`` that emits them — ``outputs`` is resolved against ``con`` (which already has the
    present parquet registered as views), so optional-gated steps only claim the relations they built.
    """
    desc: dict[str, str] = {ds.table: ds.description for ds in DATASETS}
    for step in STEPS:
        for name in step.outputs(con, resolutions):
            desc.setdefault(name, step.description)
    return desc


def build_index(extract_dir: Path, transform_dir: Path, out_path: Path, *, resolutions: list[int] | None = None) -> int:
    """Write ``out_path`` cataloguing every parquet under ``extract_dir`` + ``transform_dir``.

    Each present ``*.parquet`` is registered as a view in a throwaway in-memory DuckDB (no spatial
    extension needed — geometry stays an unread BLOB), introspected for its row count and schema, and
    joined to its registry description. Returns the number of tables indexed.
    """
    resolutions = resolutions or list(H3_RESOLUTIONS)
    con = duckdb.connect()
    try:
        # (phase, name, path) for every present parquet, registered as a view so it can be introspected
        # and so the transform steps' `outputs` see their source tables as present.
        entries: list[tuple[str, str, Path]] = []
        for phase, d in (("extract", extract_dir), ("transform", transform_dir)):
            for parquet in sorted(d.glob("*.parquet")):
                name = parquet.stem
                # inline the path (DuckDB won't prepare a CREATE VIEW); posix path, quotes escaped
                literal = parquet.as_posix().replace("'", "''")
                con.execute(f"CREATE OR REPLACE VIEW \"{name}\" AS SELECT * FROM read_parquet('{literal}')")
                entries.append((phase, name, parquet))

        descriptions = _descriptions(con, resolutions)

        rows = []
        for phase, name, parquet in entries:
            schema = con.execute(f'DESCRIBE SELECT * FROM "{name}"').fetchall()
            columns = [col[0] for col in schema]
            n_rows = con.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]  # ty:ignore[not-subscriptable]
            rows.append(
                {
                    "phase": phase,
                    "name": name,
                    "description": descriptions.get(name, ""),
                    "n_rows": n_rows,
                    "n_columns": len(columns),
                    "has_geometry": "geom" in columns,
                    "columns": ",".join(columns),
                    # the parquet's mtime = when the table was last built (sync preserves it)
                    "last_modified": datetime.fromtimestamp(parquet.stat().st_mtime, tz=UTC),
                }
            )
    finally:
        con.close()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=_COLUMNS).to_parquet(out_path, index=False)
    return len(rows)
