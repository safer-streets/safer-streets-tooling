"""Build ``index.parquet`` — a catalogue describing every extract + transform table.

The index is a small, geometry-free parquet with one row per ``*.parquet`` present under the extract and
transform directories. Each row carries the table's ``phase`` (``extract`` / ``transform``), ``name``,
a one-line ``description``, its ``n_rows`` / ``n_columns`` / ``columns`` schema summary, whether it
carries geometry, whether it is ``local_only``, and ``last_modified`` — the parquet's mtime (UTC), i.e.
when the table was last built (``sync`` preserves it across machines) — so a consumer can discover the
build outputs without opening every file.

``local_only`` is :func:`safer_streets_tooling.local_only.is_local_only` — the same predicate ``sync``
filters on, so the flag cannot drift from what actually reaches the container. The catalogue therefore
lists every table that was built, and the flag says which of them exist on this machine only: a consumer
reading the index from the blob container can tell a table it cannot find there from one that was never
built at all. A local-only row carries a **blank** ``description`` — the catalogue is synced, and the
description is the one field that says what the table *is*, so a flagged row records only that the
table exists and is not in the container.

Descriptions are otherwise the single source of truth on the registries: ``Dataset.description`` for
extract tables and ``TransformStep.description`` for the relations a transform step emits. Keep those
fields current when a table changes and this catalogue follows automatically (see AGENTS.md).
"""

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd

from safer_streets_tooling.extract import DATASETS
from safer_streets_tooling.local_only import is_local_only
from safer_streets_tooling.transform import STEPS

INDEX_NAME = "index"

# Column order of the emitted index.parquet.
_COLUMNS = [
    "phase",
    "name",
    "description",
    "n_rows",
    "n_columns",
    "has_geometry",
    "local_only",
    "columns",
    "last_modified",
]


def _descriptions(con: duckdb.DuckDBPyConnection) -> dict[str, str]:
    """Map each table name to its registry description.

    Extract tables come straight from ``Dataset.description``; transform tables from the
    ``TransformStep`` that emits them — ``outputs`` is resolved against ``con`` (which already has the
    present parquet registered as views), so optional-gated steps only claim the relations they built.
    Every step is asked, whatever ``--grid`` the last build used: the catalogue describes the parquet on
    disk, which may have been built a grid at a time.
    """
    desc: dict[str, str] = {ds.table: ds.description for ds in DATASETS}
    for step in STEPS:
        for name in step.outputs(con):
            desc.setdefault(name, step.description)
    return desc


def build_index(extract_dir: Path, transform_dir: Path, out_path: Path) -> int:
    """Write ``out_path`` cataloguing every parquet under ``extract_dir`` + ``transform_dir``.

    Each present ``*.parquet`` is registered as a view in a throwaway in-memory DuckDB (no spatial
    extension needed — geometry stays an unread BLOB), introspected for its row count and schema, and
    joined to its registry description. Local-only tables are catalogued like any other but flagged by the
    ``local_only`` column and left without a description. Returns the number of tables indexed.
    """
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

        descriptions = _descriptions(con)

        rows = []
        for phase, name, parquet in entries:
            schema = con.execute(f'DESCRIBE SELECT * FROM "{name}"').fetchall()
            columns = [col[0] for col in schema]
            n_rows = con.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]  # ty:ignore[not-subscriptable]
            # the same predicate `sync` filters on, so the flag and what reaches the container can
            # never drift apart
            local_only = is_local_only(name)
            rows.append(
                {
                    "phase": phase,
                    "name": name,
                    # the description is the one field that says what the table *is*, so it is dropped
                    # for a local-only table: the catalogue still travels, and it need only record that
                    # the table exists and is not in the container
                    "description": "" if local_only else descriptions.get(name, ""),
                    "n_rows": n_rows,
                    "n_columns": len(columns),
                    "has_geometry": "geom" in columns,
                    "local_only": local_only,
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
