# Journal — `safer-streets-tooling`

The task/design log for this repo, newest first. Every task/PR gets an entry recording **Why**,
**What**, **Design decisions**, and **Follow-ups** — see
[Task & Design Summaries](AGENTS.md#task--design-summaries) in [AGENTS.md](AGENTS.md) for the rules.
Write the entry as part of the change, not after the fact.

<!-- New entries go directly below this line. -->

## BEAHIV crime counts (`crime_counts_beahiv_202`)

**Why** — the analysis surface is H3, but we want the same crime counts on the BEAHIV equal-area
hexagonal grid (`../beahiv`) to compare the two griddings on identical data. A 202 m side gives a
cell of ~0.106 km², within a percent of an H3 resolution-9 cell, so the comparison is like for like.

**What** — a new `beahiv_counts` transform step building `crime_counts_beahiv_202` with the same
`spatial_id` / `crime_type` / `month` / `count` schema, the same BTP + un-geolocated exclusions and
the same conservation check as `crime_counts_h3_*`. Cell ids are assigned by a vectorised
(`type="arrow"`) DuckDB UDF wrapping beahiv's `bng_to_cell`. `crime_counts._CRIME_FILTER` became
public `CRIME_FILTER` so both steps share one definition of "a countable crime" rather than
duplicating it. On the full extract: 17,483,220 crimes over 221,357 cells in 3.9s (vs 234,853 cells
and 2.1s for H3 res 9 — the UDF is ~1.9x the cost of the native C extension, not the order of
magnitude a per-row Python UDF would be).

**Design decisions**

- *Vectorised UDF, not a scalar one.* A `type="arrow"` UDF is handed a whole 2048-row DuckDB vector
  as pyarrow arrays, so beahiv's numpy path runs once per vector instead of once per row: 977 Python
  calls per 2M rows rather than 2,000,000. A scalar UDF also holds the GIL per row, which would
  serialise the whole query.
- *`bng_to_cell` off `crime_data.geom`, not `latlon_to_cell` off lat/lon.* This is forced, not
  merely preferable: **calling pyproj from DuckDB's worker threads segfaults the process** (exit
  139, reproducible on the full extract). It survives only at `threads = 1`, and neither a lock
  around the transform nor a thread-local `Transformer` avoids it — the numpy half of the same
  pipeline is fine in parallel, so pyproj is the culprit. The transform phase runs with
  `threads = 4`. Going via BNG also skips a reprojection of coordinates the extractor already
  projected once, at roughly half the time. Verified identical cell ids to `latlon_to_cell` on 2M
  rows before switching; a test asserts beahiv's scalar and vector overloads still agree.
- *`spatial_id` as 16-char lowercase hex, not `UBIGINT`.* Keeps one `spatial_id` type across every
  grid (the H3 tables store `lower(hex(...))`), so consumers don't special-case this table. The raw
  ids exceed `int64`, so the numeric alternative would have forced an unsigned column through
  parquet and every downstream reader. `int(spatial_id, 16)` recovers what beahiv's `decode` takes;
  `beahiv_counts.cell_ids` does that for a list.
- *Register the UDF only when the catalog lacks it.* The obvious idempotency idiom —
  `remove_function` then `create_function` — does not work: once the UDF has executed over real
  data, `remove_function` deregisters only the Python side while `duckdb_functions()` still lists
  the name, so the re-`create_function` raises `CatalogException`. Testing the catalog instead is
  safe because the encoding is fixed by `SIDE_LENGTH` / `ORIENTATION`, so any existing registration
  is the same function. A test covers the rebuild path.
- *Not in the minimal consumer database.* Like `streetlight_counts_h3_9`, it is built by every
  transform run but left out of `_minimal_tables` — this is a comparison grid, not (yet) the
  analysis surface. `--include crime_counts_beahiv_202` pulls it in.

**Follow-ups**

- The grid is a single hard-coded `SIDE_LENGTH`. If more than one side length is ever wanted, the
  step needs parameterising the way `resolutions` parameterises the H3 steps (`resolutions` is
  currently ignored here — the grid is metres, not an H3 resolution).
- Worth an upstream beahiv issue: `latlon_to_cell` / `latlon_to_cell_batch` are documented for the
  bulk case ("encoding a whole crime dataset at once") but cannot be called from a parallel query
  engine. A pyproj-free encode path, or a documented warning, would help the next caller.
- The `h3_*_geogs` per-cell attributes have no BEAHIV equivalent, so this table currently joins to
  nothing. Whether the lookups should be generalised over grids is a design-review question.

## Per-ONS-geography crime counts (`crime_counts_{key}`)

**Why** — the H3 grids are the analysis surface, but consumers also need crime counts on the standard
ONS reporting geographies (PFA / LAD / MSOA / LSOA / OA) without re-deriving them from points.

**What** — the `crime_counts` transform step now also builds one table per ONS geography,
`crime_counts_{key}` (e.g. `crime_counts_lsoa21cd`), with the same schema as the H3 tables
(`spatial_id`, `crime_type`, `month`, `count`) and the same BTP/un-geolocated exclusions, by
point-in-polygon joining each crime's BNG `geom` to the boundary table. The tables are cached as
parquet like every step output and included in the minimal consumer database.

**Design decisions**

- *Point-in-polygon spatial join, not attributes or H3 roll-up.* `falls_within` is the reporting
  force (an attribute, not a location), and aggregating `crime_counts_h3_*` through the
  `h3_*_{key}_lookup` views would be approximate (max-overlap, resolution-dependent) and circular —
  those lookups depend on `crime_counts`. The direct join is exact and treats all five layers
  identically. `crime_data` already carries a BNG point `geom` from the extractor, so no reprojection
  is needed.
- *`ST_Contains(boundary, point)`, not `ST_Intersects`.* The ONS layers tile without overlap, but a
  point exactly on a shared edge would be counted in both areas by `ST_Intersects`; `ST_Contains`
  drops it instead — undercounting by a measure-zero case beats silent inflation.
- *Conservation is an upper bound here, not an equality.* The H3 counts must sum exactly to the
  filtered input (every crime lands in one cell); the geography layers don't cover every crime
  (PFA / MSOA / LSOA / OA are E&W-only while `crime_data` includes NI, and snapped points can sit
  just offshore of generalised boundaries), so the build raises only when a table counts *more*
  crimes than passed the filter (double counting) and prints per-layer coverage otherwise.
- *Same step, not a new one.* The outputs are crime counts with the same schema and filter; a
  separate step would duplicate the filter/conservation logic and add a DAG node for no concurrency
  gain. The step's `extract_inputs` gains the five boundary tables so staleness tracking still works.
- *`GEOGRAPHY_MAPPINGS` stays in `geo_lookups`* and is imported by `crime_counts` (no import cycle;
  smallest diff). Hoisting it to `base.py` was considered and rejected as churn for no behaviour.

**Follow-ups** — none. Note `crime_counts` now hard-requires the five boundary tables (they are
`optional=False` extracts, so this only affects standalone in-memory builds without boundaries).

---

---

## Pre-journal history

The entries below were migrated from the former `Contributions` log in AGENTS.md and predate the
why / what / design-decisions / follow-ups format.

- **Population extracts + `population_counts` transform** (#14) — Census 2021 WP001 workplace
  population and TS001 residential population per OA (nomis), assigned to buildings by floor area ×
  use weight (workplace → Non Residential/Mixed, residential → Residential/Mixed, mixed 50-50) and
  summed per res-9 cell as `population_counts_h3_9` (bundled in the default DB).
- **Table catalogue `index.parquet`** (#11) — a required one-line `description` on every `Dataset` /
  `TransformStep`, and a `data index` command (run by `assemble` / `build`) that writes
  `data_dir()/index.parquet` cataloguing every extract + transform table (phase, name, description,
  schema, geometry flag).
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
