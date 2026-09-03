# Journal — `safer-streets-tooling`

The task/design log for this repo, newest first. Every task/PR gets an entry recording **Why**,
**What**, **Design decisions**, and **Follow-ups** — see
[Task & Design Summaries](AGENTS.md#task--design-summaries) in [AGENTS.md](AGENTS.md) for the rules.
Write the entry as part of the change, not after the fact.

<!-- New entries go directly below this line. -->

## Drop H3 r8/r10; aggregate onto the Home Office hotspot hexes

**Why** — resolutions 8 and 10 tripled the transform's cost and parquet footprint for grids nobody
consumes. What is wanted instead is the grid the Home Office hotspot analysis uses: the 4,433 350m hex
cells flagged as hotspots (supplied as a GeoParquet, columns `hex_index` / `pfa` / `hits`, BNG). A hex
is ~105,400 m² — near enough an H3 r9 cell (~105,300 m²) that the two grids are directly comparable.

**What** — `H3_RESOLUTIONS` is now `[9]` (and the CLI defaults follow the constant instead of repeating
`[8, 9, 10]`). A new optional `hotspots` extract loads the polygons (renaming `hex_index` →
`spatial_id`), and three new transform steps — `hotspot_counts` → `hotspot_lookups` → `hotspot_geogs` —
rebuild the whole per-cell family on them: `crime_counts_hotspots`,
`{streetlight,building,population,road_intersection}_counts_hotspots`, `hotspots_{key}_lookup`,
`hotspots_{name}_lookup`, `hotspots_retail_centre_lookup` and `hotspots_geogs`. Everything is keyed by
`spatial_id`, so consumers join the hotspot family exactly as they join the H3 one.

**Design decisions**

- *A `SpatialUnit` rather than a parallel set of copied modules.* Every relation name is
  `<family>_h3_{res}` or `h3_{res}_<family>`, so treating `h3_9` as a *unit key* reproduces today's
  names exactly and `hotspots` gives the new ones for free. `SpatialUnit` holds only what the per-cell
  SQL varies on — the key, a subquery yielding each cell's `spatial_id` + BNG `cell_geom`, and the cell
  area expression — so `geo_lookups` / `overlap_lookups` / `retail_centre_lookups` / `geogs` each got one
  `build_unit` that both families call. No lookup or geogs SQL is duplicated.
- *…but the counts keep two code paths.* Placing a *point* differs fundamentally between the units: H3
  uses `h3_latlng_to_cell` or a precomputed `h3_9_id` column, a hex needs a point-in-polygon join. A
  single abstraction would have forced the H3 counts through geometry (slower, and for `crime_counts` a
  WGS-84 → BNG → WGS-84 round trip on the input it currently reads as lat/lon). So each counts module
  keeps its `build` and gained a `build_hotspots`, and `hotspot_counts` only wires them together —
  ~10 lines of SQL each, against rewriting the paths the H3 outputs depend on.
- *Three hotspot steps, not nine.* One step per counts module would have doubled the DAG for no
  concurrency gain (they share one connection anyway) and split `index.parquet` descriptions
  needlessly; one step per *family* (counts / lookups / geogs) mirrors the H3 subgraph's shape and gives
  each hotspot relation an independent parquet cache. `hotspot_geogs` is the only one with a
  `depends_on`: the hex cells come from the extract, not from `crime_counts`, so the counts and lookups
  are independent roots.
- *The hexes enter through the extract phase.* Reading `data_dir()/ho/hotspots.geoparquet` directly from
  a transform step would have bypassed the architecture: as a `Dataset` it is RTree-indexed by
  `index_geometry_tables`, staleness-tracked via `extract_inputs`, catalogued in `index.parquet`, synced
  to blob, and — being `optional` — its absence makes every hotspot step a clean no-op, exactly like the
  other licensed/manual layers. The path lives in `config/data_sources.json`, not in code.
- *The hotspot population allocation places buildings with an **outer** join.* The OA shares are
  normalised over whatever building set the query is given, so filtering to in-hex buildings first would
  hand a hex the *whole* OA's population instead of its share. Buildings outside the grid therefore stay
  in the window and are dropped afterwards (`WHERE spatial_id IS NOT NULL`). The H3 path is unaffected
  (every building has a cell) and now shares the same `_allocation_sql`.
- *Hotspot crime counts reuse the ONS geography count and its check.* The hexes are just another
  non-overlapping polygon layer, so `ST_Contains` + the "counted in more than one area" upper bound
  apply unchanged. Note the limitation: that bound is against the *whole* filtered input, and the
  hotspot grid covers a small fraction of it, so it would only catch gross double-counting. The supplied
  grid was checked for overlaps (none) when this was written.
- *The H3 cells subquery now de-duplicates ids before materialising boundaries* (what
  `retail_centre_lookups` already did) rather than `DISTINCT`-ing over geometry in the geography and
  overlap lookups. Same result, less work — worth having given the recent OOM tuning.
- *Fixed the `geogs` base geography while generalising it.* `_BASE_KEY` was `"lad24"`, which is not a
  key of `GEOGRAPHY_MAPPINGS` (`"lad24cd"` is), so the `else next(iter(...))` fallback silently based
  every `*_geogs` table on the **first** mapping — the E&W-only PFA layer — instead of the full-UK LAD
  layer its docstring promised. Cells covered by a LAD but no PFA were dropped: `h3_9_geogs` gains
  12,524 rows (222,397 → 234,921), all Northern Ireland, now carrying `lad24cd` with the E&W-only codes
  NULL. `hotspots_geogs` is unchanged (the hex grid is E&W-only anyway). The fallback is gone —
  a `_BASE_KEY` that isn't in the mapping now raises at import, the way the other registry
  invariants do — and the column order changes (`lad24cd` now precedes `pfa23cd`; consumers select by
  name). A regression test pins the promise: a Scotland/NI cell present only in the LAD lookup survives
  with the other codes NULL.

**Follow-ups**

- The r8/r10 parquet already written under `data_dir()/transform` (and their blob copies) are now
  orphaned — nothing rebuilds them, but `index.parquet` will keep cataloguing the local ones until they
  are deleted by hand.
- `building_counts` and `population_counts` each spatially join the buildings layer against the hexes;
  if that proves slow at full scale, materialise one building → hex lookup and share it.
- The `hits` letters (which offence classes flagged a hex) are carried through unparsed; if consumers
  need per-class hotspot flags, split them into boolean columns in the extract.

## BEAHIV 202m hex grid extract (`beahiv_202`)

**Why** — evaluation work in `safer-streets-eda` needs the BEAHIV 202m hex grid over England & Wales
as a first-class build output rather than a notebook cell rebuilt by hand each session. Consumers also
need to know *how much* of a cell falls in each police force area, so counts on a boundary cell can be
apportioned instead of double-counted.

**What** — a new extract dataset, `beahiv_202`, derived from the `police_force_areas` boundaries (no
download). One row per (cell, force): `spatial_id` (the beahiv cell id), `proportion` (share of the
cell's area inside that force), `pfa24cd`, `pfa24nm`, `geom` (the hex outline). Roughly 1.5m rows over
~1.46m distinct cells — a cell straddling a boundary appears once per force it touches, and its
proportions sum to 1 (less on a coast, where the remainder is sea).

**Design decisions**

- *Extract, not transform.* The grid is a spatial unit, matching how the Home Office `hotspots` hex
  grid is treated, even though it is derived rather than downloaded. Rejected putting it in
  `transform/` — nothing aggregates onto it yet, and its inputs are boundaries, not counts.
- *`proportion` computed in two stages.* Which cells straddle a boundary comes from one prepared
  `contains_properly` pass (~0.1s for 100k cells). The alternative — a second
  `polyfill(predicate="full")` diffed against the overlap set — gives the identical set but costs
  another full polyfill pass (~10s for the largest force), and the cell polygons are needed anyway.
- *Quadtree tiling before clipping.* Only the ~4% straddling cells need a real clip, but clipping each
  against a force's whole outline is O(its vertices): 44s for Devon & Cornwall's 4,064 edge cells
  against its 813k-vertex boundary. Subdividing the outline once into <=5k-vertex tiles and clipping
  against an STRtree over those is 2.4s, and agrees with the direct clip to 1.3e-9 m². The tiles are
  disjoint, so per-cell areas simply add.
- *Recursion terminates on a progress check, not a depth cap.* Clipping adds vertices along the cut, so
  a quadrant can come back no simpler than its parent (a box is 5 coordinates however finely it is
  split; so is a cluster of near-coincident vertices). Such a part is taken as a leaf, which keeps the
  vertex count strictly decreasing down every recursive path. A depth cap was rejected as it either
  fires too early on real data or still explodes to 4^depth tiles.
- *Rows inserted per force off an Arrow table.* A row-wise `executemany` over ~1.5m rows is far slower,
  and cannot infer the parameter type `ST_GeomFromText` needs.
- *Geometry is stored.* Every hex outline is recoverable from its cell id via `bh.cell_polygon`, so the
  `geom` column is redundant and makes the parquet large. Kept for consistency with every other
  geometry dataset (and so it is RTree-indexed on assemble); dropping it is a cheap change later.
- *beahiv becomes a tooling dependency* (`../beahiv`, editable path source), alongside
  `safer-streets-core`. It could not go in core — AGENTS.md forbids changes there.
- *Optional.* Nothing depends on `beahiv_202` yet, so a failure should not abort the build.

**Follow-ups**

- Not wired into the transform phase: no `crime_counts_beahiv_202` or equivalent. If the grid becomes a
  third spatial unit alongside H3 and the hotspot hexes, that is a separate change.
- The extract is single-threaded per force and takes ~2 minutes for all 43; it holds one
  `asyncio.to_thread` worker for that time but does not block other datasets.
- `SIDE_LENGTH` / `ORIENTATION` are module constants. If other resolutions are wanted, they should
  become dataset parameters (and the name pattern `beahiv_{side_length}` already anticipates that).

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
