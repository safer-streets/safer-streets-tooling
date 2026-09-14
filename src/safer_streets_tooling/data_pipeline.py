"""
Build the production GeoParquet outputs from modular, per-dataset parquet intermediates.

The pipeline has two phases (extract → transform):

  1. **extract**    each dataset (``safer_streets_tooling.extract.DATASETS``) is downloaded and
     preprocessed in its own in-memory DuckDB and dumped to a ``<name>.parquet`` GeoParquet file under
     ``data_dir()/extract``. The extractors run concurrently as nodes in an ``AsyncPipeline`` (see
     ``safer_streets_tooling.extract.pipeline``), respecting ``depends_on`` edges. The parquet files are
     a durable, per-dataset cache: a single dataset can be refreshed without touching the others.
  2. **transform**  the extracted parquet are loaded into a throwaway in-memory DuckDB, geometry is
     indexed, and the aggregation steps (``safer_streets_tooling.transform.STEPS``) are built; every
     derived relation (the BTP-filtered ``*_crime_counts``, the per-cell lookups and the ``*_geogs``)
     is written out as its own parquet under ``data_dir()/transform``. ``--grid`` narrows the run to
     one or more of the three grid families (``h3`` / ``ho`` / ``beahiv``); by default all three are
     built. The parquet are a durable cache of the aggregations, so they can be rebuilt without
     re-importing or re-extracting.

The parquet **are** the deliverable: consumers query them directly with an in-memory DuckDB, locally
or from the Azure blob container ``sync`` maintains. ``build`` runs both phases; ``transform`` runs the
transform alone over already-extracted parquet. Every command that (re)builds parquet
(``extract`` / ``transform`` / ``build``) then rewrites ``index.parquet`` (also available standalone as
``index``) — a catalogue with one row per extract + transform table (phase, name, description, schema;
see ``safer_streets_tooling.index``).

``sync`` reconciles the extract + transform parquet (and the ``index.parquet`` catalogue) with the
``phase2`` Azure Blob Storage container (account URL from ``SAFER_STREETS_BLOB_STORAGE``); it is
independent of the build phases. Most policies are upload-only; ``--update newer`` is a two-way sync
(upload if local is newer, download if remote is). The hotspot tables never take part in it — see
``safer_streets_tooling.local_only``.

Adding a dataset: write a module under ``safer_streets_tooling/extract/`` exposing a ``DATASET`` and
register it in ``safer_streets_tooling/extract/__init__.py``. Then ``data extract --only <name>``
and ``data transform``.
"""

import os
from pathlib import Path

import typer
from safer_streets_core.database import (
    duckdb_connector,
    index_geometry_tables,
    read_geoparquet,
)
from safer_streets_core.file_storage import AzureBlobStorage, DataSource, UpdatePolicy, blob_mtime
from safer_streets_core.utils import blob_storage_url, data_dir

from safer_streets_tooling.extract import BY_NAME, DATASETS, ExtractContext, run_extract
from safer_streets_tooling.index import INDEX_NAME, build_index
from safer_streets_tooling.local_only import is_local_only
from safer_streets_tooling.transform import ALL_GRIDS, STEPS, Grid, build_all

app = typer.Typer(help="Build the crime + boundaries + per-cell GeoParquet outputs from per-dataset intermediates.")


def extract_dir() -> Path:
    """Directory holding the per-dataset extract parquet intermediates (durable cache)."""
    d = data_dir() / "extract"
    d.mkdir(parents=True, exist_ok=True)
    return d


def transform_dir() -> Path:
    """Directory holding the aggregation parquet produced by the transform phase (durable cache)."""
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


# DuckDB worker threads for the transform phase. The DuckDB default (one per core) lets the
# concurrent spatial-join steps hold per-thread operator state across every core at once, which on
# a modest machine exhausts RAM + swap and gets the build OOM-killed.
TRANSFORM_THREADS = 4


def _transform_memory_limit() -> str:
    """Conservative DuckDB memory cap for the transform: half of physical RAM (DuckDB's default is
    80%). The headroom covers what DuckDB's accounting can't see — the Python process itself and the
    spatial extension's GEOS allocations — so hitting the cap spills to disk instead of the OOM
    killer reaping the process."""
    try:
        total = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (AttributeError, ValueError, OSError):  # non-POSIX fallback
        return "8GB"
    return f"{max(1, total // 2 // 2**30)}GB"


def _configure_transform_db(con, tdir: Path, threads: int = TRANSFORM_THREADS, memory_limit: str | None = None) -> None:
    """Bound the transform DB's resource usage (see ``TRANSFORM_THREADS`` / ``_transform_memory_limit``).

    An in-memory DuckDB gets no spill directory by default, so without one an operator that hits the
    memory limit fails instead of spilling; ``.duckdb_spill`` under ``tdir`` is ignored by ``sync``
    (which only picks up ``*.parquet``). ``preserve_insertion_order`` is dropped so large scans,
    aggregations and parquet writes stream instead of buffering to preserve row order (none of the
    transform outputs are order-sensitive)."""
    memory_limit = memory_limit or _transform_memory_limit()
    spill = tdir / ".duckdb_spill"
    spill.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET threads = {threads}")
    con.execute(f"SET memory_limit = '{memory_limit}'")
    con.execute(f"SET temp_directory = '{spill.as_posix()}'")
    con.execute("SET preserve_insertion_order = false")
    print(f"  DuckDB limits: threads={threads}, memory_limit={memory_limit}, spill dir: {spill}")


def run_transform(
    edir: Path,
    tdir: Path,
    grids: list[Grid],
    *,
    rebuild: bool = False,
    threads: int = TRANSFORM_THREADS,
    memory_limit: str | None = None,
) -> None:
    """Build the hex aggregation parquet under ``tdir`` from the extracted dataset parquet in ``edir``.

    The extracted base tables are imported into a throwaway in-memory DuckDB and geometry is indexed
    (validity repair + RTree, so the spatial joins are correct and fast). The transforms for each grid
    family in ``grids`` then run: the BTP-filtered ``*_crime_counts`` are aggregated from ``crime_data``,
    then the per-cell lookups and the ``*_geogs`` are built off them. Each transform node owns its output
    parquet under ``tdir``: a node reuses its cached output only while it is newer than
    its inputs (the extract parquet it reads + its upstream steps' outputs), else rebuilds; ``rebuild``
    forces every step. Narrowing ``grids`` leaves the other families' parquet on disk untouched — they
    are simply not rebuilt.

    ``threads`` / ``memory_limit`` bound the throwaway DuckDB's resource usage (see
    ``_configure_transform_db``): the concurrent spatial joins are memory-hungry, and the DuckDB
    defaults (all cores, 80% of RAM) can drive the build into swap and the OOM killer.
    """
    grids_label = ", ".join(grids)
    print(
        f"\n=== Transforming (extract: {edir} → transform: {tdir})"
        f" [grids: {grids_label}]{' [rebuild]' if rebuild else ''} ===\n"
    )
    con = duckdb_connector(writeable=True)  # in-memory; discarded once the parquet are written
    try:
        _configure_transform_db(con, tdir, threads, memory_limit)
        _import_datasets(con, edir)
        index_geometry_tables(con)
        build_all(STEPS, con, grids=grids, replace=False, rebuild=rebuild, edir=edir, tdir=tdir)
    finally:
        con.close()
    print(f"\n=== Done. Aggregation parquet → {tdir} ===")


def index_path() -> Path:
    """Location of the ``index.parquet`` catalogue (under ``data_dir()``, spanning both phase dirs)."""
    return data_dir() / f"{INDEX_NAME}.parquet"


def run_index(edir: Path, tdir: Path, out_path: Path) -> None:
    """Write the ``index.parquet`` catalogue describing every extract + transform table on disk."""
    print(f"\n=== Indexing extract ({edir}) + transform ({tdir}) → {out_path} ===\n")
    count = build_index(edir, tdir, out_path)
    print(f"=== Done. Catalogued {count} table(s) → {out_path} ===")


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
    run_index(ctx.staging, transform_dir(), index_path())


_THREADS_OPTION = typer.Option(
    TRANSFORM_THREADS, help="DuckDB worker threads for the transform (lower = lower peak memory)."
)
_MEMORY_LIMIT_OPTION = typer.Option(
    None, help="DuckDB memory cap for the transform, e.g. '12GB' (default: half of physical RAM)."
)
_GRID_OPTION = typer.Option(
    list(ALL_GRIDS),
    "--grid",
    help="Grid family to build, repeatable: h3 (H3 cells), ho (Home Office hotspot hexes), beahiv "
    "(BEAHIV 202m hexes), ons (OA/LSOA/MSOA/LAD/PFA). Default: all four.",
)


@app.command("transform")
def transform(
    grids: list[Grid] = _GRID_OPTION,
    all_: bool = typer.Option(False, "--all", help="Rebuild every aggregation even if its parquet exists."),
    threads: int = _THREADS_OPTION,
    memory_limit: str | None = _MEMORY_LIMIT_OPTION,
) -> None:
    """Build the hex aggregation parquet under ``data_dir()/transform`` from the extracted parquet.

    Loads the extracted datasets into a throwaway in-memory DuckDB, runs the transforms for each
    ``--grid`` family, and writes each derived relation (counts, lookups, ``*_geogs``) out as its own
    parquet. By default a node whose output parquet already exist is skipped; ``--all`` rebuilds them
    all. The parquet are the build's deliverable — consumers query them directly.
    """
    run_transform(extract_dir(), transform_dir(), grids, rebuild=all_, threads=threads, memory_limit=memory_limit)
    run_index(extract_dir(), transform_dir(), index_path())


@app.command("index")
def index() -> None:
    """Write ``index.parquet`` cataloguing every extract + transform table currently on disk.

    One row per parquet under ``data_dir()/extract`` and ``data_dir()/transform``: its phase, name,
    registry description, row/column counts, geometry flag, column list and last-modified timestamp
    (the parquet's mtime). Regenerated by every command
    that (re)builds parquet (``extract`` / ``transform`` / ``build``); run standalone to
    refresh it by hand (e.g. after deleting a parquet).
    """
    run_index(extract_dir(), transform_dir(), index_path())


@app.command("build")
def build(
    grids: list[Grid] = _GRID_OPTION,
    force_download: bool = False,
    threads: int = _THREADS_OPTION,
    memory_limit: str | None = _MEMORY_LIMIT_OPTION,
) -> None:
    """Full pass: extract any missing parquet (``--force-download`` re-fetches all), then transform.

    Cached transform parquet are kept; ``--force-download`` (which re-extracts) also rebuilds them so the
    aggregations reflect the refreshed inputs. ``--grid`` (repeatable) narrows the transform to a subset
    of the grid families.
    """
    ctx = ExtractContext(staging=extract_dir(), force_download=force_download)
    run_extract(list(DATASETS), ctx, rebuild=force_download)
    run_transform(
        ctx.staging, transform_dir(), grids, rebuild=force_download, threads=threads, memory_limit=memory_limit
    )
    run_index(ctx.staging, transform_dir(), index_path())


# Azure Blob Storage container for the phase-2 parquet. The account URL comes from the
# SAFER_STREETS_BLOB_STORAGE env var; AzureBlobStorage authenticates with a service principal
# (AZURE_* credentials — see safer_streets_core.file_storage.AzureBlobStorage).
AZURE_CONTAINER = "phase2"

# The extract + transform parquet live directly under these data_dir() subdirectories; blob names
# are the path relative to data_dir() (e.g. "extract/crime_data.parquet"). The index.parquet catalogue
# sits at the data_dir() root, so it is synced by its bare name (INDEX_NAME).
SYNC_PREFIXES: tuple[str, ...] = ("extract/", "transform/")

# Treat mtimes within this many seconds as equal: Azure last-modified is second-resolution and upload
# round-trips introduce a little jitter, so a tighter comparison would spuriously re-transfer files
# that are already in sync.
_MTIME_TOLERANCE_S = 5.0


def _local_parquet(root: Path) -> dict[str, Path]:
    """Local extract + transform parquet (+ the root ``index.parquet`` catalogue when present),
    keyed by blob name (path relative to ``root``). Local-only tables are filtered out here rather than
    at each call site, so every sync policy inherits the exclusion (see ``safer_streets_tooling.local_only``)."""
    files = {
        # as_posix() so the key matches the blob name (forward slashes) on Windows too, where
        # str(Path) would use backslashes and never match the remote "extract/..." names.
        parquet.relative_to(root).as_posix(): parquet
        for d in (extract_dir(), transform_dir())
        for parquet in d.glob("*.parquet")
        if not is_local_only(parquet.name)
    }
    index = root / f"{INDEX_NAME}.parquet"
    if index.exists():
        files[index.name] = index
    return files


def _local_only_files(root: Path) -> list[str]:
    """Blob names of the local parquet held back from the sync, so a run says what it withheld rather
    than silently doing less than it claims."""
    return sorted(
        parquet.relative_to(root).as_posix()
        for d in (extract_dir(), transform_dir())
        for parquet in d.glob("*.parquet")
        if is_local_only(parquet.name)
    )


def _remote_parquet(storage: DataSource) -> set[str]:
    """Names of the parquet blobs under the extract/ + transform/ prefixes, plus the root index.

    Local-only names are dropped from *this* side too, not just the local one: under ``--update newer`` a
    remote-only blob is downloaded, so leaving one visible would pull a local-only table back down (and, on
    a later run, push it straight back up) purely because a previous sync had put it there."""
    names = {name for prefix in SYNC_PREFIXES for name in storage.list(startswith=prefix) if name.endswith(".parquet")}
    names.update(name for name in storage.list(startswith=INDEX_NAME) if name == f"{INDEX_NAME}.parquet")
    return {name for name in names if not is_local_only(name)}


def _local_only_blobs(storage: DataSource) -> list[str]:
    """Local-only blobs already in the container — left untouched by the sync (it never deletes), but
    reported so they can be purged deliberately."""
    return sorted(
        name
        for prefix in SYNC_PREFIXES
        for name in storage.list(startswith=prefix)
        if name.endswith(".parquet") and is_local_only(name)
    )


def _download(storage: DataSource, root: Path, name: str, src_mtime: float) -> None:
    """Write the blob ``name`` to ``root / name`` and stamp it with the blob's recorded source mtime
    (so a subsequent ``newer`` sync sees the two as in-sync rather than re-transferring)."""
    dest = root / name
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(storage.read(name).getvalue())
    os.utime(dest, (src_mtime, src_mtime))


def _upload(storage: DataSource, root: Path, name: str) -> None:
    """Upload ``root / name``. ``write_file`` records the local mtime as the blob's ``src_mtime``
    metadata, so the local file and blob already agree on modification time — no re-stamping needed."""
    storage.write_file(root, name, overwrite=True)


def _sync_newer(storage: DataSource, root: Path) -> tuple[int, int, int]:
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


def _sync_upload(storage: DataSource, root: Path, update: UpdatePolicy) -> tuple[int, int]:
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

    Each ``*.parquet`` under ``data_dir()/extract`` and ``data_dir()/transform`` — plus the root
    ``index.parquet`` catalogue — is keyed by its path
    relative to ``data_dir()`` (e.g. ``extract/crime_data.parquet``). All policies except ``newer`` are
    upload-only — a blob absent remotely is uploaded, and ``--update`` decides what to do when it already
    exists. ``newer`` is a **two-way** reconcile (it also pulls down blobs newer than / missing locally):

    \b
    - ``ignore``    upload-only; skip blobs that already exist (default)
    - ``newer``     two-way: upload if local is newer, download if remote is newer
    - ``different`` upload-only; overwrite if the md5 sums differ
    - ``force``     upload-only; always overwrite

    Local-only tables — the hotspot hexes and every relation built on them, see
    ``safer_streets_tooling.local_only`` — are excluded under every policy, in both directions, and any
    already sitting in the container is reported at the end (sync itself never deletes).
    """
    account_url = blob_storage_url()
    storage = AzureBlobStorage(account_url, AZURE_CONTAINER, readonly=False)
    root = data_dir()

    arrow = "↔" if update is UpdatePolicy.NEWER else "→"
    print(f"\n=== Syncing parquet {arrow} {account_url}/{AZURE_CONTAINER} [update={update}] ===\n")
    for name in _local_only_files(root):
        print(f"    {name}: held back (local-only)")
    if update is UpdatePolicy.NEWER:
        uploaded, downloaded, skipped = _sync_newer(storage, root)
        print(f"\n=== Done. {uploaded} uploaded, {downloaded} downloaded, {skipped} skipped → {AZURE_CONTAINER} ===")
    else:
        uploaded, skipped = _sync_upload(storage, root, update)
        print(f"\n=== Done. {uploaded} uploaded, {skipped} skipped → {AZURE_CONTAINER} ===")

    # sync never deletes, so a blob uploaded before the exclusion existed would sit there unnoticed.
    if stale := _local_only_blobs(storage):
        print(f"\n!!! {len(stale)} local-only blob(s) already in {AZURE_CONTAINER} — remove them:")
        for name in stale:
            print(f"      {name}")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
