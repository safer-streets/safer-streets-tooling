# Journal — `safer-streets-tooling`

The task/design log for this repo, newest first. Every task/PR gets an entry recording **Why**,
**What**, **Design decisions**, and **Follow-ups** — see
[Task & Design Summaries](AGENTS.md#task--design-summaries) in [AGENTS.md](AGENTS.md) for the rules.
Write the entry as part of the change, not after the fact.

<!-- New entries go directly below this line. -->

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
