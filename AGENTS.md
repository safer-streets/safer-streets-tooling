# Agent Rules of Engagement — `safer-streets-tooling`

This is the rules-of-engagement doc for AI agents acting as developer, reviewer, and QA on this
repository. It says **how to work here** — the gates to pass, the invariants to preserve, and the
workflow to follow. For **what the package does** (pipeline, datasets, transform steps, modules,
usage), see [README.md](README.md); don't duplicate that material here.

In one line: `safer-streets-tooling` builds the production DuckDB database from modular, per-dataset
GeoParquet intermediates via a three-phase `extract → transform → load` pipeline, depending on
[`safer-streets-core`](../safer-streets-core) (editable path dependency) for the DuckDB helpers, the H3
transforms, the data-source catalogue, and the ONS boundary downloader.

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
  [extract/__init__.py](src/safer_streets_tooling/extract/__init__.py) **after** any `depends_on`.
  Do not add per-dataset control flow to the orchestrator. New remote URLs / filenames / layer hints
  go in core's `config/data_sources.json` (read via `data_source`), not hard-coded here.
- **Adding a transform step is additive too.** Write a module under
  [transform/](src/safer_streets_tooling/transform/) exposing `STEP = TransformStep(...)` with
  `build(con, resolutions, replace)`, `outputs(con, resolutions)`, and `depends_on`, then register it in
  [transform/__init__.py](src/safer_streets_tooling/transform/__init__.py) **after** any `depends_on`.
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
  change: [README.md](README.md) (including its extract & transform DAG mermaid diagram and the dataset
  / transform-step / module tables) and the relevant module/CLI docstrings. Docs and code must never
  drift.

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
9. **Docs** — if the pipeline, CLI flags, the dataset set, or the extract/transform DAG change, the
   docs in [README.md](README.md) (including the DAG mermaid diagram) are updated in the same change.

## QA Rules

- Run the full gate suite (`ruff check`, `ruff format --check`, `ty check`, `pytest`) before declaring
  a task done.
- The minimum supported Python is **3.13** (`requires-python = ">=3.13"`). Don't use newer syntax/stdlib.
- DuckDB file-locking and the atomic swap can behave differently on Windows (stricter about open
  handles) — flag platform-sensitive path/file-handle code.
- If a test is skipped or `xfail`, leave a comment explaining why and when it can be removed.

## Branch and Release Policy

- Create a feature branch off `main` — never commit directly to `main`.
- **Never merge a PR without explicit approval.** Open the PR, report CI status, and stop.
- `safer-streets-core` is consumed as an editable path dependency, not a pinned release. When core is
  eventually published/pinned, update `[tool.uv.sources]` / `[project.dependencies]` accordingly.
- Version bumps go in [pyproject.toml](pyproject.toml) (`version = "x.y.z"`).

## Workflow

1. Create a feature branch off `main` — never commit directly to `main`.
2. Make changes under [src/safer_streets_tooling/](src/safer_streets_tooling/); for a new dataset, add
   a module + a registry entry.
3. Add or update tests in [tests/](tests/) — offline-safe, coverage at/above 65%.
4. Run the full gate suite locally (`ruff check`, `ruff format --check`, `ty check`, `pytest`).
5. Update the docs ([README.md](README.md)) if the pipeline, CLI flags, the dataset set, or the
   extract/transform DAG changed; new data-source locations go in core's `config/data_sources.json`.
6. Commit (pre-commit hooks auto-fix formatting and re-lock `uv.lock` once configured), open a PR,
   report CI, and **stop** — do not merge without approval.

## Contributions

A log of the substantive changes made to this repo, newest first. Add an entry here when you land a PR
that changes the dataset/transform set, the pipeline, or developer-facing behaviour.

- **Buildings extract + `building_counts_h3_9` transform** (#9) — Verisk UKBuildings footprints, counted
  per resolution-9 cell split by `map_simple_use`, restricted to crime cells.
- **CCTV extract** (#8) — OSM `man_made=surveillance` via Overpass (presence/indicative signal).
- **Streetlights extract + `streetlight_counts_h3_9` transform** (#7) — Overture/OSM `street_lamp`,
  counted per resolution-9 cell.
- **`food_outlets` — drop component scores** (#6) — keep only `rating_value`.
- **`food_outlets` — broaden takeaways to food & drink venues** (#5) — generalised the FSA takeaways
  layer (#4) into `food_outlets`.
- **FSA food-hygiene takeaways (E&W) extract** (#4).
- **NAPTAN transport stops extract** (#3).
- **CI: resolve editable core path dep by sibling checkout** (#2) — plus posix-key normalisation so
  sync works on Windows.
- **OAC + OAC classification, land-cover overlap split, sync refactor** (#1).
- **Initial pipeline** — extract → transform → load with the dataset/transform registries, async DAG
  runner, `data` CLI, cell areas, `load` step, `poi` / `schools` / `imd` layers, and Azure Blob `sync`.
