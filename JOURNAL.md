# Journal — `safer-streets-tooling`

The task/design log for this repo, newest first. Every task/PR gets an entry recording **Why**,
**What**, **Design decisions**, and **Follow-ups** — see
[Task & Design Summaries](AGENTS.md#task--design-summaries) in [AGENTS.md](AGENTS.md) for the rules.
Write the entry as part of the change, not after the fact.

<!-- New entries go directly below this line. -->

## The BEAHIV grid as a third spatial unit (`crime_counts_beahiv_202`, `beahiv_202_geogs`)

**Why** — the `beahiv_202` extract landed as a grid nothing was counted onto, so the BEAHIV gridding
joined to nothing: no crime counts, no ONS codes, no overlap layers, no nearest retail centre. The
comparison the grid exists for — the same crimes and the same per-cell attributes on an equal-area
hexagonal gridding versus H3 — needs both sides built.

**What** — three steps, `beahiv_counts` → `beahiv_lookups` → `beahiv_geogs`, mirroring the hotspot
trio: `crime_counts_beahiv_202`, `beahiv_202_{key}_lookup`, `beahiv_202_{name}_lookup`,
`beahiv_202_retail_centre_lookup` and `beahiv_202_geogs` — column for column identical to
`h3_9_geogs` apart from `spatial_id`'s type, asserted by a test comparing the two schemas.
[transform/beahiv.py](src/safer_streets_tooling/transform/beahiv.py) holds the unit itself
(`BEAHIV_UNIT`, `available`, `register_udfs`), the BEAHIV counterpart of
[transform/hotspots.py](src/safer_streets_tooling/transform/hotspots.py). `crime_counts._CRIME_FILTER`
and `_expected` become the public `CRIME_FILTER` / `expected_crimes`, so both count paths share one
definition of "a countable crime" rather than duplicating it.

Verified on the full extract (17,812,176 rows, `threads = 4` as the transform phase actually runs):

| | BEAHIV 202 m | H3 res 9 |
| --- | --- | --- |
| crimes counted | 17,485,799 | 17,485,799 |
| cells | 221,429 | 234,922 |
| build time | 4.8s | 2.2s |

Identical totals, conservation check passes. The Python UDF costs ~2.1x the native C h3 extension —
not the order of magnitude a per-row UDF would. Every id came back between 1.15e18 and 1.16e18 —
just above 2**60, well inside the signed range the `BIGINT` column assumes.

The full chain then runs to `beahiv_202_geogs`: 221,428 rows over 221,428 distinct cells (one row per
cell, as the schema requires), every one with an ONS code, and a single distinct `cell_area` of
106,011.90 m² — the analytic hexagon area. 877s, which is `geogs`' usual cost on this many cells
rather than anything specific to the grid. One cell of the 221,429 doesn't reach `geogs`: it
intersects no LAD polygon, so `geo_lookups`' inner join drops it — the same behaviour the H3 grid has.

Note this run needed `ST_MakeValid` over the ONS boundaries. The 2026-09-04 boundary extracts contain
45 invalid polygons (36 OA, 7 LSOA, 1 MSOA, 1 LAD) and `ST_Intersection` against them raises
`TopologyException: side location conflict`. That is **pre-existing and not specific to this grid** —
`main`'s H3 `geogs` fails identically on the same data — so it is left to its own fix; see Follow-ups.

**Design decisions**

- *A `SpatialUnit`, not a new grid abstraction.* An earlier draft of this work introduced a parallel
  `Grid` dataclass and reworked `geo_lookups` / `overlap_lookups` / `retail_centre_lookups` / `geogs`
  to iterate grids. `SpatialUnit` and its `build_unit` hooks then landed on `main` for the hotspot
  hexes and are the same idea arrived at independently, so BEAHIV is now simply a third unit and none
  of those four modules is touched. One abstraction for "a gridding of Great Britain", not two.
- *Cells from the crime counts, not from the `beahiv_202` extract.* The extract tiles the whole of
  England & Wales (~1.5m cells); the lookups would then do a boundary intersection for every cell in
  the country, the overwhelming majority of which carry no crime. Taking the cells from
  `crime_counts_beahiv_202` makes the grid exactly the crime grid, as it already is for H3, and keeps
  the comparison like for like.
- *`spatial_id` as a plain `BIGINT`.* beahiv reserves the top three bits of a cell id, which puts
  every id below 2**61 — so a signed column holds one and `decode` takes it back with no cast. The
  earlier draft encoded ids as 16-char hex strings to keep one `spatial_id` type across every grid;
  that cost a `bytes.fromhex` decode on the way back and joined to nothing, since the `beahiv_202`
  extract keys on the integer. The extract moved `UBIGINT` → `BIGINT` to match. The consequence is
  that `beahiv_202_geogs.spatial_id` is an integer where `h3_9_geogs.spatial_id` is a hex string —
  a property of the two indexings, not of these tables, and a consumer joining counts to geogs stays
  within one grid.
- *Counting from BNG, not lat/lon.* `latlon_to_cell` reprojects with pyproj, and calling pyproj from
  DuckDB's worker threads **segfaults the process** (reproducible on the full extract; survives only
  at `threads = 1`, and neither a lock nor a thread-local `Transformer` avoids it). The transform runs
  at `threads = 4`, so that path is unusable. `crime_data.geom` is already BNG, so `bng_to_cell` is
  also the cheaper call — no reprojecting coordinates we already hold.
- *The cell centre from a UDF, the hexagon from SQL.* DuckDB has no BEAHIV cell function, so the
  geometry has to come from Python. A UDF returning the boundary as WKT — the direct analogue of the
  h3 extension's `h3_cell_to_boundary_wkt` — would mean formatting a WKT string per cell. Instead the
  UDF returns only the centre as a `STRUCT(x, y)` and the six vertices are constant offsets from it,
  so the Python side is one vectorised `centroid` call over the whole DuckDB vector with no per-row
  work at all.
- *Vertex offsets derived from beahiv, not restated.* Every cell of a given side length and
  orientation is the same hexagon translated, so the offsets come from `cell_polygon` of a reference
  cell minus its own centre rather than from a copy of beahiv's vertex-angle table. A test asserts the
  SQL polygon is vertex-for-vertex `cell_polygon`'s, which catches a wrong CRS, a swapped x/y or a
  drifted offset in one place. (`cell_polygon` now returns a Shapely `Polygon`, so the ring comes off
  `.exterior.coords` — and already carries the closing vertex `ST_MakePolygon` needs.)
- *`cell_area` as the analytic `3√3/2·s²`.* A constant, because the grid is equal-area — and
  deliberately the *planar* BNG area, the same measure as the `{prefix}_overlap_area` columns it is
  the denominator for. (The H3 units' `h3_cell_area` is geodesic m², which differs from its own planar
  BNG area by the ~0.08% grid scale factor; that inconsistency is pre-existing.) Cast to `DOUBLE`
  explicitly: DuckDB reads a bare decimal literal as `DECIMAL`, which would give the two `*_geogs`
  tables different `cell_area` types and defeat the point of matching schemas.
- *The grid's parameters live in [beahiv_grid.py](src/safer_streets_tooling/beahiv_grid.py).* The
  extract that tiles the grid and the transform that counts onto it must agree on side length and
  orientation, or they build two disjoint grids that still join on `spatial_id` without error — every
  row simply missing. One definition, imported by both phases, rather than a copy in each. It also
  breaks the import cycle that putting them in `beahiv_counts` would create (`beahiv_counts` →
  `crime_counts` → … ).
- *UDF registration is idempotent and lock-guarded.* The catalog outlives a single `build` and is
  shared by every cursor, so a rebuild or a concurrent step would otherwise fail on the name already
  existing. `remove_function` and re-create looks like the obvious fix but doesn't work: once the UDF
  has *executed* over real data it only deregisters the Python side, and `create_function` then raises
  `CatalogException`. `base.register_udf` tests the catalog and skips instead. Every step touching the
  grid calls `register_udfs`, including the ones that only read the lookups — those are *views*
  carrying the centre-UDF call, so it must be registered whenever one is evaluated.
- *Not local-only.* Unlike the hotspot family, nothing here is supplied in confidence — the grid is
  derived from public boundaries and the counts from public crime data — so the BEAHIV relations sync
  to the shared container like the H3 ones.

**Follow-ups**

- Only *crime* counts are on this grid. The other measures (`streetlight_counts`, `building_counts`,
  `population_counts`, `road_intersection_counts`) have H3 and hotspot paths but no BEAHIV one; each
  would be a `build_beahiv` alongside the existing pair, and they read a precomputed `h3_9_id` column
  the extracts write, so a BEAHIV equivalent needs either a second id column or a join through the
  cell polygons.
- `spatial_id` type now varies by grid (`VARCHAR` for H3 and hotspots, `BIGINT` for BEAHIV). Worth a
  look at whether the H3 tables should carry the id as an integer too, which would make the three
  `*_geogs` schemas identical.
- **`geogs` is currently broken on the live boundary data, for every grid.** The 2026-09-04 ONS
  extracts contain 45 invalid polygons and `ST_Intersection` against them raises a GEOS
  `TopologyException`; `main`'s H3 path fails the same way, and the last successful `h3_9_geogs` build
  (2026-09-03) predates those boundaries. `ST_MakeValid` in the boundary extractors is the likely fix.
  Deliberately not fixed here — it is neither caused by nor confined to this change.

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
