# Agent Guidelines for `safer-streets-tooling`

This file instructs AI agents acting as developer, reviewer, and QA for this repository.

## Project Overview

`safer-streets-tooling` is the data-build tooling for the Safer Streets project, split out of
[`safer-streets-core`](../safer-streets-core). It builds the production DuckDB database (police.uk
crime + ONS boundaries + supplementary geographic/deprivation layers + H3 aggregations) from
**modular, per-dataset GeoParquet intermediates**.

It depends on `safer-streets-core` (editable path dependency) for the DuckDB helpers
(`safer_streets_core.database`), the H3 transforms (`safer_streets_core.transforms`), the data-source
catalogue (`safer_streets_core.utils.data_source`, backed by core's `config/data_sources.json`), and
the ONS boundary downloader (`scripts.ons_boundaries`, which lives in core). It adds **no new data
logic of its own to core** — core stays as-is.

### Three-phase pipeline (extract → transform → load)

The `data` CLI (`extract`, `transform`, `load`, plus `assemble` = transform+load and `build` = all
three) drives a dataset registry (`safer_streets_tooling.datasets.DATASETS`):

1. **extract** — each dataset is downloaded and preprocessed in its **own in-memory DuckDB** and dumped
   to a `<name>.parquet` GeoParquet file under `data_dir()/extract` (raw source files are cached under
   `data_dir()/raw`). The extractors run **concurrently**
   as nodes in an `AsyncPipeline`, respecting `depends_on` edges. Each parquet is a durable per-dataset
   cache, so one dataset can be refreshed without rebuilding everything.
2. **transform** — the extracted parquet are loaded into a throwaway in-memory DuckDB, geometry is
   indexed, and the H3 aggregations run (`transforms.build_all`, `replace=False` so the extracted
   `crime_counts_h3_*` are kept). Every newly-derived relation (per-cell lookups + `h3_{res}_geogs`) is
   written out as its own parquet under `data_dir()/transform` — a durable cache, no live DB touched.
3. **load** — present parquet (extract + transform) are imported into a `<name>.staging.db`, geometry
   tables are repaired and RTree-indexed (`index_geometry_tables`), and the staging file is
   **atomically promoted** (`os.replace`) over the live database. Consumers only ever see a complete DB.

### Key modules

Source lives in [src/safer_streets_tooling/](src/safer_streets_tooling/):

| File | Role |
| ---- | ---- |
| [build_db.py](src/safer_streets_tooling/build_db.py) | `data` CLI: `extract` / `assemble` / `build` commands + `run_assemble` |
| [extract.py](src/safer_streets_tooling/extract.py) | Concurrent extract phase: `DatasetExtractNode`, `build_pipeline`, `run_extract` |
| [async_pipeline.py](src/safer_streets_tooling/async_pipeline.py) | DAG runner over `AsyncNode`s (`graphlib.TopologicalSorter` + `asyncio.gather`) |
| [async_node.py](src/safer_streets_tooling/async_node.py) | `AsyncNode` base: derives `dependency_ids` from `execute`'s kwonly args; `__call__` captures exceptions as `Err` |
| [result.py](src/safer_streets_tooling/result.py) | `Result[T]` / `Ok` / `Err` (`unwrap`, `is_ok`, `is_err`) |
| [datasets/base.py](src/safer_streets_tooling/datasets/base.py) | `Dataset` spec + `ExtractContext` |
| [`datasets/__init__.py`](src/safer_streets_tooling/datasets/__init__.py) | Ordered `DATASETS` registry + `BY_NAME` + dependency validation |
| [datasets/_common.py](src/safer_streets_tooling/datasets/_common.py) | `download`, `extract_cached`, `rename_geom_column`, `write_geoparquet`, `read_geoparquet` |

### Datasets

One module per dataset under [datasets/](src/safer_streets_tooling/datasets/), each exposing a
`DATASET` (or `DATASETS` for the boundary group). Registry order respects `depends_on`:

| Dataset(s) | Module | Required? | Depends on |
| ---------- | ------ | --------- | ---------- |
| `crime_data` | [crime.py](src/safer_streets_tooling/datasets/crime.py) | yes | — |
| 5 ONS boundary tables | [boundaries.py](src/safer_streets_tooling/datasets/boundaries.py) | yes | — |
| `open_greenspace` | [greenspace.py](src/safer_streets_tooling/datasets/greenspace.py) | no | — |
| `land_cover` | [land_cover.py](src/safer_streets_tooling/datasets/land_cover.py) | no | — |
| `retail_centres` | [retail_centres.py](src/safer_streets_tooling/datasets/retail_centres.py) | no | — |
| `open_roads` | [roads.py](src/safer_streets_tooling/datasets/roads.py) | no | — |
| `poi` | [poi.py](src/safer_streets_tooling/datasets/poi.py) | no | — |
| `schools` | [schools.py](src/safer_streets_tooling/datasets/schools.py) | no | `open_roads` (walk-isochrone network) |
| `imd_scores_pct` | [imd.py](src/safer_streets_tooling/datasets/imd.py) | no | `local_authority_districts` (Welsh LA-name→code lookup) |

## Toolchain

| Tool | Command |
| ---- | ------- |
| Package manager | `uv` |
| Linter / formatter | `ruff` (`uv run ruff check`, `uv run ruff format`) |
| Type checker | `ty` (`uv run ty check`) |
| Tests | `uv run pytest` |
| Install dev deps | `uv sync --group dev` |

`uv sync` installs `safer-streets-core` as an **editable path dependency** (`../safer-streets-core`,
configured under `[tool.uv.sources]`). Core must be checked out as a sibling directory.

## Quality Gates

All of the following must pass before any change is considered complete:

```sh
uv run ruff check          # zero lint errors
uv run ruff format --check # zero formatting issues
uv run ty check            # zero type errors
uv run pytest              # all tests pass, coverage >= 65%
```

`pytest` is configured (in [pyproject.toml](pyproject.toml)) with `--cov=src/safer_streets_tooling`
and `--cov-fail-under=65`. Tests live in [tests/](tests/) and must stay **offline-safe** — mock
downloads and skip when the spatial extensions can't be fetched (`duckdb.HTTPException`) or when
Overture S3 is unreachable, mirroring the existing tests.

## Developer Rules

- **Core stays as-is.** This repo must not require changes to `safer-streets-core`. Import what you
  need from `safer_streets_core.*` and from core's `scripts.ons_boundaries`. If you find yourself
  needing to edit core, stop and raise it — that crosses a repo boundary.
- **Geometry is British National Grid (EPSG:27700) everywhere.** Coordinates are the contract; CRS
  metadata is not. Sources in another CRS (e.g. retail centres in WGS-84) are reprojected to BNG
  **inside their extractor** before being written. DuckDB's GeoParquet writer tags geometry as
  `OGC:CRS84`; that label is harmless and is stripped to a bare `GEOMETRY` on assemble by
  `index_geometry_tables`. Write geometry with `write_geoparquet`; read it back with `read_geoparquet`.
- **Adding a dataset is additive.** Write a module under [datasets/](src/safer_streets_tooling/datasets/)
  exposing `DATASET = Dataset(...)` whose `extract(ctx)` does its work in its own
  `duckdb_connector()` and writes `ctx.parquet(name)`, then register it in
  [`datasets/__init__.py`](src/safer_streets_tooling/datasets/__init__.py) **after** any `depends_on`.
  Do not add per-dataset control flow to the orchestrator. New remote URLs / filenames / layer hints
  go in core's `config/data_sources.json` (read via `data_source`), not hard-coded here.
- **The assemble phase must stay atomic.** It writes a `<name>.staging.db` and only promotes it with
  `os.replace` once import + index + transforms have all succeeded. Never let a read-only consumer see
  a half-built database.
- **Extract concurrency model.** Each dataset is a `DatasetExtractNode`; its blocking work runs via
  `asyncio.to_thread`, so multiple in-memory DuckDB connections run in parallel (this is safe — each
  extractor owns its connection). `depends_on` become graph edges; under `--only` subsets, edges to
  datasets outside the set are dropped (the dependency is read from its cached parquet). `AsyncNode`
  turns any exception into an `Err`; `run_extract` re-raises only for **required** datasets and skips
  optional ones with a warning. Preserve this — a failed optional source must not abort the build.
- **Required vs optional datasets.** `optional=False` (crime, boundaries) aborts the build if it can't
  be produced; everything else is best-effort and skipped. The H3 transforms already tolerate absent
  optional tables, so a skipped dataset is simply omitted from the output.
- **Type annotations required.** Full signatures; `ty` must pass. A few rules are relaxed in
  [pyproject.toml](pyproject.toml) (`invalid-assignment`, `unresolved-attribute`,
  `no-matching-overload`) — fix types rather than widening those, using targeted `# ty: ignore[...]`.
- **Line length is 120** (`[tool.ruff]`, `E501` ignored). Active ruff rules: `A, B, E, F, I, SIM, UP`
  (`D103` ignored in `tests/`).
- **No comments explaining what the code does.** Only comment the non-obvious *why*.

## Reviewer Checklist

When reviewing a PR or diff, check:

1. **CRS correctness** — every geometry is BNG by the time it is written; non-BNG sources are
   reprojected in the extractor, not later.
2. **Registry, not control flow** — new datasets are added via a module + registry entry, with correct
   `optional`/`geometry`/`depends_on`, and `depends_on` precedes the dataset in `DATASETS`.
3. **Assemble integrity** — still staging + atomic `os.replace`? Geometry tables indexed before the
   transforms run?
4. **Extract robustness** — optional-source failures become skips, required failures abort; `--only`
   subsets don't deadlock on absent dependencies; no shared mutable state across the concurrent nodes.
5. **Offline-safe tests** — pass without network / data dir / API key; skip cleanly when extensions or
   Overture S3 are unavailable.
6. **Coverage** — change keeps total coverage at or above the 65% gate; new code paths have tests.
7. **No core edits** — the change does not depend on modifying `safer-streets-core`.
8. **Types & ruff** — precise annotations, no suppressed `select` rules without justification.
9. **Docs** — if the pipeline, CLI flags, the dataset set, or the extract/transform DAG change, update
   [README.md](README.md) (including its extract & transform DAG mermaid diagram).

## QA Rules

- Run the full gate suite (`ruff check`, `ruff format --check`, `ty check`, `pytest`) before declaring
  a task done.
- The minimum supported Python is **3.13** (`requires-python = ">=3.13"`). Don't use newer syntax/stdlib.
- DuckDB file-locking and the atomic swap can behave differently on Windows (stricter about open
  handles) — flag platform-sensitive path/file-handle code.
- If a test is skipped or `xfail`, leave a comment explaining why and when it can be removed.

## Repository Layout

```
src/
  safer_streets_tooling/
    __init__.py
    build_db.py        # `data` CLI: extract / assemble / build
    extract.py         # concurrent extract phase (AsyncPipeline wiring)
    async_pipeline.py  # DAG runner over AsyncNodes
    async_node.py      # AsyncNode base (exception-safe, dependency introspection)
    result.py          # Result / Ok / Err
    datasets/
      __init__.py      # DATASETS registry + BY_NAME + validation
      base.py          # Dataset spec + ExtractContext
      _common.py       # download / extract_cached / rename_geom_column / (read|write)_geoparquet
      crime.py boundaries.py greenspace.py land_cover.py retail_centres.py
      roads.py poi.py schools.py imd.py
tests/                 # pytest suite (offline-safe)
README.md
AGENTS.md
pyproject.toml
```

## Branch and Release Policy

- `safer-streets-core` is consumed as an editable path dependency, not a pinned release. When core is
  eventually published/pinned, update `[tool.uv.sources]` / `[project.dependencies]` accordingly.
- Version bumps go in [pyproject.toml](pyproject.toml) (`version = "x.y.z"`).

## Workflow

1. Create a feature branch off `main` — never commit directly to `main`.
2. Make changes under [src/safer_streets_tooling/](src/safer_streets_tooling/); for a new dataset, add
   a module + a registry entry.
3. Add or update tests in [tests/](tests/) — offline-safe, coverage at/above 65%.
4. Run the full gate suite locally (`ruff check`, `ruff format --check`, `ty check`, `pytest`).
5. If the pipeline, CLI flags, the dataset set, or the extract DAG changed, update
   [README.md](README.md); new data-source locations go in core's `config/data_sources.json`.
6. Commit (pre-commit hooks auto-fix formatting and re-lock `uv.lock` once configured).
