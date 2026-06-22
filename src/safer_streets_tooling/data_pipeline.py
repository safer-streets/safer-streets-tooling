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
  3. **load**       *(optional)* a minimal consumer database is assembled from the transform parquet —
     ``crime_counts_h3_{res}`` and ``h3_{res}_geogs`` (the per-cell counts + attributes, joined on
     ``spatial_id``) plus the ONS boundary tables they reference by code (PFA / LAD / MSOA / LSOA / OA)
     and the schools / poi / naptan / imd_scores_pct / land_cover / oac (+ oac_classification) feature layers —
     into a ``<name>.staging.db`` that is only promoted over the live database with an atomic
     ``os.replace`` once every table loaded, so read-only consumers always see a complete database.
     ``--include NAME`` adds further tables (an intermediate lookup or a feature layer). This step is
     optional: the parquet are the durable outputs; the database is just a convenience bundle.

``build`` runs all three phases; ``assemble`` runs transform + load over already-extracted parquet.
The live database is the standard database (``database_path()``, under ``SAFER_STREETS_DATA_DIR``);
pass ``--db-path`` to override.

``sync`` reconciles the extract + transform parquet with the ``phase2`` Azure Blob Storage container
(account URL from ``SAFER_STREETS_BLOB_STORAGE``); it is independent of the build phases. Most policies
are upload-only; ``--update newer`` is a two-way sync (upload if local is newer, download if remote is).

Adding a dataset: write a module under ``safer_streets_tooling/extract/`` exposing a ``DATASET`` and
register it in ``safer_streets_tooling/extract/__init__.py``. Then ``data extract --only <name>``
and ``data assemble``.
"""

import os
from collections.abc import Iterable
from io import BytesIO
from pathlib import Path
from typing import Any, Protocol

import typer
from safer_streets_core.database import (
    duckdb_connector,
    index_geometry_tables,
    read_geoparquet,
)
from safer_streets_core.file_storage import AzureBlobStorage, UpdatePolicy, blob_mtime
from safer_streets_core.utils import blob_storage_url, data_dir, database_path

from safer_streets_tooling.extract import BY_NAME, DATASETS, ExtractContext, run_extract
from safer_streets_tooling.transform import STEPS, build_all
from safer_streets_tooling.transform.geo_lookups import GEOGRAPHY_MAPPINGS

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


# Feature layers included in the database by default (extract datasets, loaded from ``edir``).
DEFAULT_FEATURE_TABLES: tuple[str, ...] = (
    "schools",
    "poi",
    "naptan",
    "imd_scores_pct",
    "land_cover",
    "oac",
    "oac_classification",
)


def _minimal_tables(resolutions: list[int]) -> list[str]:
    """The relations the minimal consumer database needs:

    - ``crime_counts_h3_{res}`` — per-cell crime counts (keyed by the H3 ``spatial_id``);
    - ``h3_{res}_geogs`` — per-cell attributes (also keyed by ``spatial_id``);
    - the ONS boundary tables ``h3_*_geogs`` references by code (PFA / LAD / MSOA / LSOA / OA), so a
      consumer can resolve a cell's codes to the boundary geometry;
    - the ``DEFAULT_FEATURE_TABLES`` feature layers (schools / poi / naptan / imd_scores_pct / land_cover / oac +
      ``oac_classification``; ``oac`` is the per-OA 2021 Output Area Classification code, keyed by
      ``oa21cd``, decoded to tier names via the ``oac_classification`` dimension table).

    The intermediate lookups and the other raw extract datasets are build inputs, not part of the output.
    """
    counts = [f"crime_counts_h3_{res}" for res in resolutions]
    geogs = [f"h3_{res}_geogs" for res in resolutions]
    return counts + geogs + list(GEOGRAPHY_MAPPINGS.values()) + list(DEFAULT_FEATURE_TABLES)


def run_load(
    db_path: Path, tdir: Path, resolutions: list[int], *, edir: Path | None = None, include: list[str] | None = None
) -> None:
    """Assemble a minimal consumer database from the transform parquet, then atomically promote it.

    By default the ``crime_counts_h3_{res}`` and ``h3_{res}_geogs`` parquet (under ``tdir``) plus the ONS
    boundary tables they reference by code (PFA / LAD / MSOA / LSOA / OA, under ``edir``) and the
    ``DEFAULT_FEATURE_TABLES`` feature layers (schools / poi / naptan / imd_scores_pct / land_cover / oac (+ oac_classification), under ``edir``) are
    imported — the per-cell counts and attributes the app joins on ``spatial_id``, the boundaries those
    cells resolve to, and the feature layers. ``include`` names further tables to add (each looked up
    under ``tdir`` then ``edir``) —
    e.g. an intermediate ``h3_*_lookup`` or a feature layer. The boundary tables' geometry is repaired
    and RTree-indexed; the counts/geogs carry none. A table backed by an *optional* dataset (e.g. the
    licensed ``land_cover`` extract) is skipped with a warning when its parquet is absent; a missing
    *required* parquet aborts. The staging DB is only promoted over ``db_path`` with ``os.replace`` once
    every present table loaded, so consumers only ever see a complete database.

    This load step is **optional**: the per-dataset and transform parquet are the durable build outputs;
    the database is just a convenience bundle for consumers that prefer a single file.
    """
    search_dirs = [d for d in (tdir, edir) if d is not None]
    tables = _minimal_tables(resolutions) + (include or [])
    # tables backed by an optional dataset are skipped with a warning when absent (e.g. the licensed
    # land_cover extract); a missing required table still aborts the build.
    optional_tables = {ds.table for ds in DATASETS if ds.optional}

    staging = db_path.with_suffix(".staging.db")
    staging.unlink(missing_ok=True)

    print(f"\n=== Loading {db_path} (staging: {staging}) ===\n")
    con = duckdb_connector(staging, writeable=True)
    loaded = 0
    try:
        for name in tables:
            parquet = next((d / f"{name}.parquet" for d in search_dirs if (d / f"{name}.parquet").exists()), None)
            if parquet is None:
                searched = ", ".join(str(d) for d in search_dirs)
                if name in optional_tables:
                    print(f"  {name}: optional parquet absent, skipping (searched: {searched})")
                    continue
                raise FileNotFoundError(f"required table '{name}' parquet not found in: {searched}")
            con.execute(f'CREATE OR REPLACE TABLE "{name}" AS {read_geoparquet(parquet)}')
            loaded += 1
            print(f"  {name}: loaded")

        index_geometry_tables(con)  # no-op for the minimal tables (no geometry); indexes any included layers
    finally:
        con.close()

    os.replace(staging, db_path)
    print(f"\n=== Done. Promoted minimal database ({loaded} table(s)) → {db_path} ===")


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
    resolutions: list[int] = [8, 9, 10],  # noqa: B006
    include: list[str] | None = None,
) -> None:
    """Assemble a minimal database from the transform parquet, then atomically swap it in.

    By default ``crime_counts_h3_{res}`` and ``h3_{res}_geogs`` (the per-cell counts + attributes, joined
    on ``spatial_id``) plus the ONS boundary tables they reference by code (PFA / LAD / MSOA / LSOA / OA)
    and the schools / poi / naptan / imd_scores_pct / land_cover / oac (+ oac_classification) feature layers are imported. ``--include NAME`` (repeatable)
    adds further tables (an intermediate ``h3_*_lookup`` or a feature layer), looked up in the transform
    then extract dirs. This step is optional — the parquet are the durable outputs; the database is a
    convenience bundle.
    """
    run_load(db_path or database_path(), transform_dir(), resolutions, edir=extract_dir(), include=include)


@app.command("assemble")
def assemble(
    db_path: Path | None = None,
    resolutions: list[int] = [8, 9, 10],  # noqa: B006
    all_: bool = typer.Option(False, "--all", help="Rebuild every aggregation even if its parquet exists."),
    include: list[str] | None = None,
) -> None:
    """Transform then load: build the H3 aggregation parquet, then assemble + promote the minimal database.

    ``--include NAME`` (repeatable) adds extra tables to the database beyond the minimal set.
    """
    run_transform(extract_dir(), transform_dir(), resolutions, rebuild=all_)
    run_load(db_path or database_path(), transform_dir(), resolutions, edir=extract_dir(), include=include)


@app.command("build")
def build(
    db_path: Path | None = None,
    resolutions: list[int] = [8, 9, 10],  # noqa: B006
    force_download: bool = False,
    include: list[str] | None = None,
) -> None:
    """Full pass: extract any missing parquet (``--force-download`` re-fetches all), then transform + load.

    Cached transform parquet are kept; ``--force-download`` (which re-extracts) also rebuilds them so the
    aggregations reflect the refreshed inputs. ``--include NAME`` (repeatable) adds extra tables to the
    database beyond the minimal set.
    """
    ctx = ExtractContext(staging=extract_dir(), force_download=force_download)
    run_extract(list(DATASETS), ctx, rebuild=force_download)
    run_transform(ctx.staging, transform_dir(), resolutions, rebuild=force_download)
    run_load(db_path or database_path(), transform_dir(), resolutions, edir=ctx.staging, include=include)


# Azure Blob Storage container for the phase-2 parquet. The account URL comes from the
# SAFER_STREETS_BLOB_STORAGE env var; AzureBlobStorage authenticates with a service principal
# (AZURE_* credentials — see safer_streets_core.file_storage.AzureBlobStorage).
AZURE_CONTAINER = "phase2"

# The extract + transform parquet live directly under these data_dir() subdirectories; blob names
# are the path relative to data_dir() (e.g. "extract/crime_data.parquet").
SYNC_PREFIXES: tuple[str, ...] = ("extract/", "transform/")

# Treat mtimes within this many seconds as equal: Azure last-modified is second-resolution and upload
# round-trips introduce a little jitter, so a tighter comparison would spuriously re-transfer files
# that are already in sync.
_MTIME_TOLERANCE_S = 5.0


class _BlobStore(Protocol):
    """The blob-storage surface the sync helpers use (satisfied structurally by ``AzureBlobStorage``);
    typing against it keeps the reconcile logic testable with an in-memory fake."""

    def list(self, startswith: str | None = None) -> Iterable[str]: ...
    def read(self, filename: str) -> BytesIO: ...
    def metadata(self, filename: str) -> Any: ...
    def write_file(self, root_path: Path, filename: str, *, overwrite: bool = False) -> bool: ...
    def needs_update(self, root_path: Path, filename: str, policy: UpdatePolicy) -> bool: ...


def _local_parquet(root: Path) -> dict[str, Path]:
    """Local extract + transform parquet, keyed by blob name (path relative to ``root``)."""
    return {
        # as_posix() so the key matches the blob name (forward slashes) on Windows too, where
        # str(Path) would use backslashes and never match the remote "extract/..." names.
        parquet.relative_to(root).as_posix(): parquet
        for d in (extract_dir(), transform_dir())
        for parquet in d.glob("*.parquet")
    }


def _remote_parquet(storage: _BlobStore) -> set[str]:
    """Names of the parquet blobs under the extract/ + transform/ prefixes."""
    return {name for prefix in SYNC_PREFIXES for name in storage.list(startswith=prefix) if name.endswith(".parquet")}


def _download(storage: _BlobStore, root: Path, name: str, src_mtime: float) -> None:
    """Write the blob ``name`` to ``root / name`` and stamp it with the blob's recorded source mtime
    (so a subsequent ``newer`` sync sees the two as in-sync rather than re-transferring)."""
    dest = root / name
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(storage.read(name).getvalue())
    os.utime(dest, (src_mtime, src_mtime))


def _upload(storage: _BlobStore, root: Path, name: str) -> None:
    """Upload ``root / name``. ``write_file`` records the local mtime as the blob's ``src_mtime``
    metadata, so the local file and blob already agree on modification time — no re-stamping needed."""
    storage.write_file(root, name, overwrite=True)


def _sync_newer(storage: _BlobStore, root: Path) -> tuple[int, int, int]:
    """Two-way reconcile of the extract + transform parquet by modification time: upload local-only
    and locally-newer files, download remote-only and remotely-newer ones. Returns (up, down, skipped)."""
    local = _local_parquet(root)
    remote = _remote_parquet(storage)
    uploaded = downloaded = skipped = 0
    for name in sorted(set(local) | remote):
        meta = storage.metadata(name) if name in remote else None
        if meta is None:  # local-only
            _upload(storage, root, name)
            print(f"  ↑ {name}: uploaded (local only)")
            uploaded += 1
        elif name not in local:  # remote-only
            _download(storage, root, name, blob_mtime(meta))
            print(f"  ↓ {name}: downloaded (remote only)")
            downloaded += 1
        else:
            delta = local[name].stat().st_mtime - blob_mtime(meta)
            if delta > _MTIME_TOLERANCE_S:
                _upload(storage, root, name)
                print(f"  ↑ {name}: uploaded (local newer)")
                uploaded += 1
            elif delta < -_MTIME_TOLERANCE_S:
                _download(storage, root, name, blob_mtime(meta))
                print(f"  ↓ {name}: downloaded (remote newer)")
                downloaded += 1
            else:
                print(f"    {name}: skipped (in sync)")
                skipped += 1
    return uploaded, downloaded, skipped


def _sync_upload(storage: _BlobStore, root: Path, update: UpdatePolicy) -> tuple[int, int]:
    """Upload local parquet, deferring the overwrite-or-skip decision for existing blobs to ``update``.
    Returns (uploaded, skipped)."""
    uploaded = skipped = 0
    for name in sorted(_local_parquet(root)):
        if storage.needs_update(root, name, update):
            # write_file records the local mtime as the blob's src_mtime, so a later `--update newer`
            # compares like for like and won't mistake the freshly-uploaded remote for being newer.
            _upload(storage, root, name)
            print(f"  ↑ {name}: uploaded")
            uploaded += 1
        else:
            print(f"    {name}: skipped")
            skipped += 1
    return uploaded, skipped


@app.command("sync")
def sync(
    update: UpdatePolicy = typer.Option(  # noqa: B008
        UpdatePolicy.IGNORE, help="How to reconcile parquet that exist on both sides."
    ),
) -> None:
    """Sync the extract + transform parquet with Azure Blob Storage (``phase2`` container).

    Each ``*.parquet`` under ``data_dir()/extract`` and ``data_dir()/transform`` is keyed by its path
    relative to ``data_dir()`` (e.g. ``extract/crime_data.parquet``). All policies except ``newer`` are
    upload-only — a blob absent remotely is uploaded, and ``--update`` decides what to do when it already
    exists. ``newer`` is a **two-way** reconcile (it also pulls down blobs newer than / missing locally):

    \b
    - ``ignore``    upload-only; skip blobs that already exist (default)
    - ``newer``     two-way: upload if local is newer, download if remote is newer
    - ``different`` upload-only; overwrite if the md5 sums differ
    - ``force``     upload-only; always overwrite
    """
    account_url = blob_storage_url()
    storage = AzureBlobStorage(account_url, AZURE_CONTAINER, readonly=False)
    root = data_dir()

    arrow = "↔" if update is UpdatePolicy.NEWER else "→"
    print(f"\n=== Syncing parquet {arrow} {account_url}/{AZURE_CONTAINER} [update={update}] ===\n")
    if update is UpdatePolicy.NEWER:
        uploaded, downloaded, skipped = _sync_newer(storage, root)
        print(f"\n=== Done. {uploaded} uploaded, {downloaded} downloaded, {skipped} skipped → {AZURE_CONTAINER} ===")
    else:
        uploaded, skipped = _sync_upload(storage, root, update)
        print(f"\n=== Done. {uploaded} uploaded, {skipped} skipped → {AZURE_CONTAINER} ===")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
