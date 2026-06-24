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
three) drives a dataset registry (`safer_streets_tooling.extract.DATASETS`) and a transform-step
registry (`safer_streets_tooling.transform.STEPS`):

1. **extract** — each dataset is downloaded and preprocessed in its **own in-memory DuckDB** and dumped
   to a `<name>.parquet` GeoParquet file under `data_dir()/extract` (raw source files are cached under
   `data_dir()/raw`). The extractors run **concurrently**
   as nodes in an `AsyncPipeline`, respecting `depends_on` edges. Each parquet is a durable per-dataset
   cache, so one dataset can be refreshed without rebuilding everything.
2. **transform** — the extracted parquet are loaded into a throwaway in-memory DuckDB, geometry is
   indexed, and the H3 aggregation steps run concurrently as nodes in an `AsyncPipeline`
   (`transform.build_all(STEPS, con, …)`), respecting `depends_on` edges. Each step writes the relations
   it produces (the BTP-filtered `crime_counts_h3_*`, per-cell lookups, `h3_{res}_geogs`) out as its own
   parquet under `data_dir()/transform` — a durable cache; a step whose output parquet already exist is
   skipped unless `--all`. No live DB is touched.
3. **load** *(optional)* — a **minimal** database is assembled in a `<name>.staging.db` from
   `crime_counts_h3_{res}` + `h3_{res}_geogs` (joined on `spatial_id`) plus the five ONS boundary tables
   their codes resolve to (PFA / LAD / MSOA / LSOA / OA, from the extract dir), then **atomically
   promoted** (`os.replace`) over the live database so consumers only ever see a complete DB. `--include
   NAME` adds non-default tables (a lookup or a feature layer, looked up in the transform then extract
   dir); boundary geometry is RTree-indexed. Optional because the parquet are the durable outputs — the
   DB is a convenience bundle.

### Key modules

Source lives in [src/safer_streets_tooling/](src/safer_streets_tooling/):

| File | Role |
| ---- | ---- |
| [data_pipeline.py](src/safer_streets_tooling/data_pipeline.py) | `data` CLI: `extract` / `assemble` / `build` commands + `run_assemble` |
| [extract/pipeline.py](src/safer_streets_tooling/extract/pipeline.py) | Concurrent extract phase: `DatasetExtractNode`, `build_pipeline`, `run_extract` |
| [transform/pipeline.py](src/safer_streets_tooling/transform/pipeline.py) | Concurrent transform phase: `TransformNode`, `build_pipeline`, `build_all` |
| [async_pipeline.py](src/safer_streets_tooling/async_pipeline.py) | DAG runner over `AsyncNode`s (`graphlib.TopologicalSorter` + `asyncio.gather`) |
| [async_node.py](src/safer_streets_tooling/async_node.py) | `AsyncNode` base: derives `dependency_ids` from `execute`'s kwonly args; `__call__` captures exceptions as `Err` |
| [result.py](src/safer_streets_tooling/result.py) | `Result[T]` / `Ok` / `Err` (`unwrap`, `is_ok`, `is_err`) |
| [extract/base.py](src/safer_streets_tooling/extract/base.py) | `Dataset` spec + `ExtractContext` |
| [`extract/__init__.py`](src/safer_streets_tooling/extract/__init__.py) | Ordered `DATASETS` registry + `BY_NAME` + dependency validation |
| [extract/_common.py](src/safer_streets_tooling/extract/_common.py) | `download`, `extract_cached`, `rename_geom_column`, `write_geoparquet`, `read_geoparquet` |
| [transform/base.py](src/safer_streets_tooling/transform/base.py) | `TransformStep` spec + `create_clause` / `table_exists` helpers |
| [`transform/__init__.py`](src/safer_streets_tooling/transform/__init__.py) | Ordered `STEPS` registry + `BY_NAME` + dependency validation |

### Datasets

One module per dataset under [extract/](src/safer_streets_tooling/extract/), each exposing a
`DATASET` (or `DATASETS` for the boundary group). Registry order respects `depends_on`:

| Dataset(s) | Module | Required? | Depends on |
| ---------- | ------ | --------- | ---------- |
| `crime_data` | [crime.py](src/safer_streets_tooling/extract/crime.py) | yes | — |
| 5 ONS boundary tables | [boundaries.py](src/safer_streets_tooling/extract/boundaries.py) | yes | — |
| `open_greenspace` | [greenspace.py](src/safer_streets_tooling/extract/greenspace.py) | no | — |
| `land_cover` | [land_cover.py](src/safer_streets_tooling/extract/land_cover.py) | no | — |
| `buildings` | [buildings.py](src/safer_streets_tooling/extract/buildings.py) | no | — |
| `retail_centres` | [retail_centres.py](src/safer_streets_tooling/extract/retail_centres.py) | no | — |
| `open_roads` | [roads.py](src/safer_streets_tooling/extract/roads.py) | no | — |
| `poi` | [poi.py](src/safer_streets_tooling/extract/poi.py) | no | — |
| `naptan` | [naptan.py](src/safer_streets_tooling/extract/naptan.py) | no | — |
| `food_outlets` | [food_outlets.py](src/safer_streets_tooling/extract/food_outlets.py) | no | — |
| `streetlights` | [streetlights.py](src/safer_streets_tooling/extract/streetlights.py) | no | — |
| `cctv` | [cctv.py](src/safer_streets_tooling/extract/cctv.py) | no | — |
| `schools` | [schools.py](src/safer_streets_tooling/extract/schools.py) | no | `open_roads` (walk-isochrone network) |
| `imd_scores_pct` | [imd.py](src/safer_streets_tooling/extract/imd.py) | no | `local_authority_districts` (Welsh LA-name→code lookup) |
| `oac`, `oac_classification` | [oac.py](src/safer_streets_tooling/extract/oac.py) | no | — |

### Transform steps

One module per step under [transform/](src/safer_streets_tooling/transform/), each exposing a `STEP`.
Registry order respects `depends_on`:

| Step | Module | Outputs | Depends on |
| ---- | ------ | ------- | ---------- |
| `crime_counts` | [crime_counts.py](src/safer_streets_tooling/transform/crime_counts.py) | `crime_counts_h3_{res}` | — |
| `streetlight_counts` | [streetlight_counts.py](src/safer_streets_tooling/transform/streetlight_counts.py) | `streetlight_counts_h3_9` | — |
| `building_counts` | [building_counts.py](src/safer_streets_tooling/transform/building_counts.py) | `building_counts_h3_9` (by `map_simple_use`) | `crime_counts` |
| `geo_lookups` | [geo_lookups.py](src/safer_streets_tooling/transform/geo_lookups.py) | `h3_{res}_{key}_lookup` | `crime_counts` |
| `overlap_lookups` | [overlap_lookups.py](src/safer_streets_tooling/transform/overlap_lookups.py) | `h3_{res}_{name}_lookup` | `crime_counts` |
| `retail_centre_lookups` | [retail_centre_lookups.py](src/safer_streets_tooling/transform/retail_centre_lookups.py) | `h3_{res}_retail_centre_lookup` | `crime_counts` |
| `geogs` | [geogs.py](src/safer_streets_tooling/transform/geogs.py) | `h3_{res}_geogs` | `geo_lookups`, `overlap_lookups`, `retail_centre_lookups` |

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
- **Adding a dataset is additive.** Write a module under [extract/](src/safer_streets_tooling/extract/)
  exposing `DATASET = Dataset(...)` whose `extract(ctx)` does its work in its own
  `duckdb_connector()` and writes `ctx.parquet(name)`, then register it in
  [`extract/__init__.py`](src/safer_streets_tooling/extract/__init__.py) **after** any `depends_on`.
  Do not add per-dataset control flow to the orchestrator. New remote URLs / filenames / layer hints
  go in core's `config/data_sources.json` (read via `data_source`), not hard-coded here.
- **Adding a transform step is additive too.** Write a module under
  [transform/](src/safer_streets_tooling/transform/) exposing `STEP = TransformStep(...)` with
  `build(con, resolutions, replace)`, `outputs(con, resolutions)`, and `depends_on`, then register it in
  [`transform/__init__.py`](src/safer_streets_tooling/transform/__init__.py) **after** any `depends_on`.
  The pipeline caches each step by its declared `outputs`, so keep `outputs` in step with what `build`
  creates.
- **The assemble phase must stay atomic.** It writes a `<name>.staging.db` and only promotes it with
  `os.replace` once import + index + transforms have all succeeded. Never let a read-only consumer see
  a half-built database.
- **Extract concurrency model.** Each dataset is a `DatasetExtractNode`; its blocking work runs via
  `asyncio.to_thread`, so multiple in-memory DuckDB connections run in parallel (this is safe — each
  extractor owns its connection). `depends_on` become graph edges; under `--only` subsets, edges to
  datasets outside the set are dropped (the dependency is read from its cached parquet). `AsyncNode`
  turns any exception into an `Err`; `run_extract` re-raises only for **required** datasets and skips
  optional ones with a warning. Preserve this — a failed optional source must not abort the build.
- **Transform concurrency model.** All steps share one in-memory connection but each `TransformNode`
  runs its build on its own `con.cursor()` (safe for concurrent DDL); `depends_on` become graph edges,
  so the three lookups run concurrently off `crime_counts` and `geogs` waits for them. A step caches its
  declared `outputs` as parquet and, when they all exist (and not `--all`), reloads them instead of
  rebuilding — so a downstream step can still read an upstream step's relations from the catalog.
  `build_all` unwraps each result, re-raising the first failure.
- **Required vs optional datasets.** `optional=False` (crime, boundaries) aborts the build if it can't
  be produced; everything else is best-effort and skipped. The H3 transforms already tolerate absent
  optional tables, so a skipped dataset is simply omitted from the output.
- **Type annotations required.** Full signatures; `ty` must pass. A few rules are relaxed in
  [pyproject.toml](pyproject.toml) (`invalid-assignment`, `unresolved-attribute`,
  `no-matching-overload`) — fix types rather than widening those, using targeted `# ty: ignore[...]`.
- **Line length is 120** (`[tool.ruff]`, `E501` ignored). Active ruff rules: `A, B, E, F, I, SIM, UP`
  (`D103` ignored in `tests/`).
- **No comments explaining what the code does.** Only comment the non-obvious *why*.
- **Keep documentation in step with the code.** Any change to the pipeline, CLI commands/flags, the
  dataset or transform-step set, the DAG, or developer-facing behaviour must update the docs in the same
  change: [README.md](README.md) (including its extract & transform DAG mermaid diagram), this
  `AGENTS.md`, and the relevant module/CLI docstrings. Docs and code must never drift.

## Reviewer Checklist

When reviewing a PR or diff, check:

1. **CRS correctness** — every geometry is BNG by the time it is written; non-BNG sources are
   reprojected in the extractor, not later.
2. **Registry, not control flow** — new datasets / transform steps are added via a module + registry
   entry, with correct `optional`/`geometry`/`depends_on` (datasets) or `outputs`/`depends_on` (steps),
   and each `depends_on` precedes its entry in `DATASETS` / `STEPS`.
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
    data_pipeline.py   # `data` CLI: extract / transform / load / assemble / build / sync
    async_pipeline.py  # DAG runner over AsyncNodes
    async_node.py      # AsyncNode base (exception-safe, dependency introspection)
    result.py          # Result / Ok / Err
    config.py          # data-source catalogue accessor
    extract/
      __init__.py      # DATASETS registry + BY_NAME + validation
      pipeline.py      # concurrent extract phase (AsyncPipeline wiring)
      base.py          # Dataset spec + ExtractContext
      _common.py       # download / extract_cached / rename_geom_column / (read|write)_geoparquet
      crime.py boundaries.py greenspace.py land_cover.py buildings.py retail_centres.py
      roads.py poi.py naptan.py food_outlets.py streetlights.py cctv.py schools.py imd.py oac.py
    transform/
      __init__.py      # STEPS registry + BY_NAME + validation
      pipeline.py      # concurrent transform phase (TransformNode + AsyncPipeline wiring)
      base.py          # TransformStep spec + create_clause / table_exists helpers
      crime_counts.py streetlight_counts.py building_counts.py geo_lookups.py overlap_lookups.py retail_centre_lookups.py geogs.py
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
