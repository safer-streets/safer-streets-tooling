"""
Build the production DuckDB database from modular, per-dataset parquet intermediates.

The pipeline has three phases (extract → transform → load):

  1. **extract**    each dataset (``safer_streets_tooling.extract.DATASETS``) is downloaded and
     preprocessed in its own in-memory DuckDB and dumped to a ``<name>.parquet`` GeoParquet file under
     ``data_dir()/extract``. The extractors run concurrently as nodes in an ``AsyncPipeline`` (see
     ``safer_streets_tooling.extract.pipeline``), respecting ``depends_on`` edges. The parquet files are
     a durable, per-dataset cache: a single dataset can be refreshed without touching the others.
  2. **transform**  the extracted parquet are loaded into a throwaway in-memory DuckDB, geometry is
     indexed, and the H3 aggregation steps (``safer_streets_tooling.transform.STEPS``) are built; every
     derived relation (the BTP-filtered ``crime_counts_h3_*``, the per-cell lookups and
     ``h3_{res}_geogs``) is written out as its own parquet under ``data_dir()/transform``. No live
     database is touched — the parquet are a durable cache of the aggregations, so they can be rebuilt
     without re-importing or re-extracting.
  3. **load**       every present parquet (extracted datasets + transform aggregations) is imported
     into a ``<name>.staging.db``, geometry tables are repaired + RTree-indexed, and the staging file
     is only promoted over the live database with an atomic ``os.replace`` once every step has
     succeeded, so read-only consumers always see a complete database — either the old one or the new
     one, never a half-built file.

``build`` runs all three phases; ``assemble`` runs transform + load over already-extracted parquet.
The live database is the standard database (``database_path()``, under ``SAFER_STREETS_DATA_DIR``);
pass ``--db-path`` to override.

Adding a dataset: write a module under ``safer_streets_tooling/extract/`` exposing a ``DATASET`` and
register it in ``safer_streets_tooling/extract/__init__.py``. Then ``data extract --only <name>``
and ``data assemble``.
"""

import os
from pathlib import Path

import typer
from safer_streets_core.database import (
    duckdb_connector,
    index_geometry_tables,
    read_geoparquet,
)
from safer_streets_core.utils import data_dir, database_path

from safer_streets_tooling.extract import BY_NAME, DATASETS, ExtractContext, run_extract
from safer_streets_tooling.transform import STEPS, build_all

app = typer.Typer(help="Build the crime + boundaries + H3 DuckDB database from per-dataset parquet intermediates.")


def extract_dir() -> Path:
    """Directory holding the per-dataset extract parquet intermediates (durable cache)."""
    d = data_dir() / "extract"
    d.mkdir(parents=True, exist_ok=True)
    return d


def transform_dir() -> Path:
    """Directory holding the H3 aggregation parquet produced by the transform phase (durable cache)."""
    d = data_dir() / "transform"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _import_datasets(con, sdir: Path) -> int:
    """Import every present dataset parquet from ``sdir`` as a table, aborting if a *required* one is
    missing. Returns the number imported."""
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
        imported += 1
    return imported


def run_transform(edir: Path, tdir: Path, resolutions: list[int], *, rebuild: bool = False) -> None:
    """Build the H3 aggregation parquet under ``tdir`` from the extracted dataset parquet in ``edir``.

    The extracted base tables are imported into a throwaway in-memory DuckDB and geometry is indexed
    (validity repair + RTree, so the spatial joins are correct and fast). The H3 transforms then run:
    the BTP-filtered ``crime_counts_h3_*`` are aggregated from ``crime_data``, then the per-cell lookups
    and ``h3_{res}_geogs`` are built off them. Each transform node owns its output parquet under ``tdir``
    (a durable cache the *load* step imports): a node reuses its cached output only while it is newer than
    its inputs (the extract parquet it reads + its upstream steps' outputs), else rebuilds; ``rebuild``
    forces every step. No live database is touched.
    """
    print(f"\n=== Transforming (extract: {edir} → transform: {tdir}){' [rebuild]' if rebuild else ''} ===\n")
    con = duckdb_connector(writeable=True)  # in-memory; discarded once the parquet are written
    try:
        _import_datasets(con, edir)
        index_geometry_tables(con)
        build_all(STEPS, con, resolutions=resolutions, replace=False, rebuild=rebuild, edir=edir, tdir=tdir)
    finally:
        con.close()
    print(f"\n=== Done. H3 aggregation parquet → {tdir} ===")


def run_load(db_path: Path, edir: Path, tdir: Path) -> None:
    """Import every present dataset parquet (``edir``) and H3 aggregation parquet (``tdir``) into a
    staging DB, index geometry tables, then atomically promote the staging DB over ``db_path``."""
    staging = db_path.with_suffix(".staging.db")
    staging.unlink(missing_ok=True)

    print(f"\n=== Loading {db_path} (staging: {staging}) ===\n")
    con = duckdb_connector(staging, writeable=True)
    try:
        imported = _import_datasets(con, edir)
        transformed = 0
        for parquet in sorted(tdir.glob("*.parquet")):
            con.execute(f'CREATE OR REPLACE TABLE "{parquet.stem}" AS {read_geoparquet(parquet)}')
            transformed += 1

        print(f"\nImported {imported} dataset + {transformed} transform table(s); validating + indexing geometry…")
        index_geometry_tables(con)
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
    """(Re)build parquet intermediates under ``data_dir()/extract``.

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

    ctx = ExtractContext(staging=extract_dir(), force_download=force_download)
    run_extract(targets, ctx, rebuild=rebuild)


@app.command("transform")
def transform(
    resolutions: list[int] = [8, 9, 10],  # noqa: B006
    all_: bool = typer.Option(False, "--all", help="Rebuild every aggregation even if its parquet exists."),
) -> None:
    """Build the H3 aggregation parquet under ``data_dir()/transform`` from the extracted parquet.

    Loads the extracted datasets into a throwaway in-memory DuckDB, runs the H3 transforms, and writes
    each derived relation (lookups + ``h3_{res}_geogs``) out as its own parquet. By default a node whose
    output parquet already exist is skipped; ``--all`` rebuilds them all. No live database is touched;
    ``load`` imports the result.
    """
    run_transform(extract_dir(), transform_dir(), resolutions, rebuild=all_)


@app.command("load")
def load(
    db_path: Path | None = None,
) -> None:
    """Build the database from whatever parquet exist (extract + transform), then atomically swap it in.

    Each present dataset and H3 aggregation parquet is imported as a table; geometry tables are repaired
    and RTree-indexed. A missing *required* dataset (e.g. crime, boundaries) aborts.
    """
    run_load(db_path or database_path(), extract_dir(), transform_dir())


@app.command("assemble")
def assemble(
    db_path: Path | None = None,
    resolutions: list[int] = [8, 9, 10],  # noqa: B006
    all_: bool = typer.Option(False, "--all", help="Rebuild every aggregation even if its parquet exists."),
) -> None:
    """Transform then load: build the H3 aggregation parquet, then assemble + promote the database."""
    run_transform(extract_dir(), transform_dir(), resolutions, rebuild=all_)
    run_load(db_path or database_path(), extract_dir(), transform_dir())


@app.command("build")
def build(
    db_path: Path | None = None,
    resolutions: list[int] = [8, 9, 10],  # noqa: B006
    force_download: bool = False,
) -> None:
    """Full pass: extract any missing parquet (``--force-download`` re-fetches all), then transform + load.

    Cached transform parquet are kept; ``--force-download`` (which re-extracts) also rebuilds them so the
    aggregations reflect the refreshed inputs.
    """
    ctx = ExtractContext(staging=extract_dir(), force_download=force_download)
    run_extract(list(DATASETS), ctx, rebuild=force_download)
    run_transform(ctx.staging, transform_dir(), resolutions, rebuild=force_download)
    run_load(db_path or database_path(), ctx.staging, transform_dir())


def main() -> None:
    app()


if __name__ == "__main__":
    main()
