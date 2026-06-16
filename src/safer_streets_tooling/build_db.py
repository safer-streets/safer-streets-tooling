"""
Build the production DuckDB database from modular, per-dataset parquet intermediates.

The pipeline has two phases:

  1. **extract**   each dataset (``safer_streets_tooling.datasets.DATASETS``) is downloaded and
     preprocessed in its own in-memory DuckDB and dumped to a ``<name>.parquet`` GeoParquet file under
     ``data_dir()/build``. The extractors run concurrently as nodes in an ``AsyncPipeline`` (see
     ``safer_streets_tooling.extract``), respecting ``depends_on`` edges. The parquet files are a
     durable, per-dataset cache: a single dataset can be refreshed without touching the others.
  2. **assemble**  the final database is built by importing those parquet files into a
     ``<name>.staging.db``, repairing + RTree-indexing geometry tables, and running the H3 transforms
     (``safer_streets_core.transforms``). The staging file is only promoted to the live database with
     an atomic ``os.replace`` once every step has succeeded, so read-only consumers always see a
     complete database — either the old one or the new one, never a half-built file.

``build`` runs both phases (extract any missing parquet, then assemble). The live database is the
standard database (``database_path()``, under ``SAFER_STREETS_DATA_DIR``); pass ``--db-path`` to override.

Adding a dataset: write a module under ``safer_streets_tooling/datasets/`` exposing a ``DATASET`` and
register it in ``safer_streets_tooling/datasets/__init__.py``. Then ``data extract --only <name>``
and ``data assemble``.
"""

import os
from pathlib import Path

import typer
from safer_streets_core import transforms
from safer_streets_core.database import duckdb_connector, index_geometry_tables
from safer_streets_core.utils import data_dir, database_path

from safer_streets_tooling.datasets import BY_NAME, DATASETS, ExtractContext
from safer_streets_tooling.datasets._common import read_geoparquet
from safer_streets_tooling.extract import run_extract

app = typer.Typer(help="Build the crime + boundaries + H3 DuckDB database from per-dataset parquet intermediates.")


def staging_dir() -> Path:
    """Directory holding the per-dataset parquet intermediates (durable cache)."""
    d = data_dir() / "build"
    d.mkdir(parents=True, exist_ok=True)
    return d


def run_assemble(db_path: Path, sdir: Path, resolutions: list[int]) -> None:
    """Import every present dataset parquet from ``sdir`` into a staging DB, index geometry tables,
    build the H3 transforms, then atomically promote the staging DB over ``db_path``."""
    staging = db_path.with_suffix(".staging.db")
    staging.unlink(missing_ok=True)

    print(f"\n=== Assembling {db_path} (staging: {staging}) ===\n")
    con = duckdb_connector(staging, writeable=True)
    try:
        imported = 0
        for ds in DATASETS:
            parquet = sdir / f"{ds.name}.parquet"
            if not parquet.exists():
                if not ds.optional:
                    raise FileNotFoundError(
                        f"required dataset '{ds.name}' parquet missing: {parquet}\nRun `data extract` first."
                    )
                print(f"  {ds.name}: parquet absent, skipping")
                continue
            con.execute(f'CREATE OR REPLACE TABLE "{ds.table}" AS {read_geoparquet(parquet)}')
            print(f"  {ds.table}: imported")
            imported += 1

        print(f"\nImported {imported} table(s); validating geometries, indexing and building H3 aggregations…")
        index_geometry_tables(con)
        transforms.build_all(con, resolutions=resolutions)
    finally:
        con.close()

    os.replace(staging, db_path)
    print(f"\n=== Done. Promoted staging database → {db_path} ===")


@app.command("extract")
def extract(
    only: list[str] | None = None,
    force_download: bool = False,
    all_: bool = typer.Option(False, "--all", help="Re-extract every dataset even if its parquet exists."),
) -> None:
    """(Re)build parquet intermediates under ``data_dir()/build``.

    By default only *missing* parquet are built. ``--only NAME`` (repeatable) rebuilds specific
    datasets; ``--force-download`` re-fetches sources and rebuilds; ``--all`` rebuilds everything from
    the cached downloads.
    """
    if only:
        unknown = [n for n in only if n not in BY_NAME]
        if unknown:
            raise typer.BadParameter(f"unknown dataset(s): {', '.join(unknown)}. Known: {', '.join(BY_NAME)}")
        targets = [BY_NAME[n] for n in only]
        rebuild = True
    else:
        targets = list(DATASETS)
        rebuild = force_download or all_

    ctx = ExtractContext(staging=staging_dir(), force_download=force_download)
    run_extract(targets, ctx, rebuild=rebuild)


@app.command("assemble")
def assemble(
    db_path: Path | None = None,
    resolutions: list[int] = [8, 9, 10],  # noqa: B006
) -> None:
    """Build the database from whatever parquet exist, then atomically swap it into place.

    Each present dataset parquet is imported as a table; geometry tables are repaired and RTree-indexed;
    the H3 transforms are (re)built. A missing *required* dataset (e.g. crime, boundaries) aborts.
    """
    run_assemble(db_path or database_path(), staging_dir(), resolutions)


@app.command("build")
def build(
    db_path: Path | None = None,
    resolutions: list[int] = [8, 9, 10],  # noqa: B006
    force_download: bool = False,
) -> None:
    """Full pass: extract any missing parquet (``--force-download`` re-fetches all), then assemble."""
    ctx = ExtractContext(staging=staging_dir(), force_download=force_download)
    run_extract(list(DATASETS), ctx, rebuild=force_download)
    run_assemble(db_path or database_path(), ctx.staging, resolutions)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
