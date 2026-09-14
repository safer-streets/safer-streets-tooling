# Journal — `safer-streets-tooling`

The task/design log for this repo, newest first. Every task/PR gets an entry recording **Why**,
**What**, **Design decisions**, and **Follow-ups** — see
[Task & Design Summaries](AGENTS.md#task--design-summaries) in [AGENTS.md](AGENTS.md) for the rules.
Write the entry as part of the change, not after the fact.

<!-- New entries go directly below this line. -->

## Fix: the four new BEAHIV counts were built but never written to parquet

**Why** — after re-extracting with `beahiv202_id` on every point layer, `data transform --grid beahiv`
produced `beahiv202_crime_counts.parquet` and the lookups/geogs, but no
`beahiv202_building_counts.parquet`, `beahiv202_streetlight_counts.parquet`,
`beahiv202_population_counts.parquet` or `beahiv202_road_intersection_counts.parquet` — silently. No
error, no warning; the tables existed in the in-memory catalog (`beahiv_counts.build` did call each
`build_beahiv`) but were never among the relations `TransformNode` wrote out.

**Root cause** — `TransformNode._run` resolves `step.outputs(cur)` *before* calling `step.build(cur,
replace)`, to decide which relation names to cache/write. `beahiv.tagged()` — the gate `build_beahiv`
and `beahiv_outputs` share on the other three counts modules — was defined as
`available(con) and table_exists(...) and column_exists(...)`, and `available()` tests whether
`beahiv202_crime_counts` exists in the catalog. At the point `outputs()` is called, `beahiv_counts.build`
has not run yet in this pass, so the crime counts don't exist, `available()` is False, `tagged()` is
False, and every one of the four `beahiv_outputs()` returned `[]`. The node then built all five
relations (crime counts plus the four) inside `beahiv_counts.build`, but only ever intended to persist
the one name it had already decided on. `road_intersection_counts.beahiv_outputs` had the identical
bug via a direct `beahiv.available(con)` check.

**Fix** — `beahiv.tagged()` no longer checks `available()`; it tests only that the source table exists
and carries the id column, which is everything a `GROUP BY {grid}_id` actually needs and is knowable
before any BEAHIV step has run. `road_intersection_counts` drops its `beahiv.available()` check
outright, for the same reason — nothing about it reads the crime counts any more since the previous
entry removed that restriction.

**Design decisions**

- **A `TransformStep`'s `outputs()` must be true independent of that step's own `build()` having run.**
  This is the general lesson, not specific to BEAHIV: any gate inside `outputs()` that can only become
  true as a *side effect of `build()`* will read False at exactly the moment the pipeline asks it,
  because `outputs()` always runs first. `available()` was fine for every existing caller (the lookup
  and geogs steps, which run in later, separate steps after `beahiv_counts` has already built and been
  cached) — it only broke for a check inside the *same* step that creates the thing being checked.

**Verified**: a regression test constructs exactly the failing precondition — a connection with the
source layer present and tagged, but `beahiv202_crime_counts` absent — and asserts `beahiv_outputs()`
lists the table anyway. Checked against the pre-fix code that it does fail there
(`[] == ['beahiv202_building_counts']`), confirming the test actually exercises the bug.

## The BEAHIV grid gets the other four counts

**Why** — the grid had crime counts, lookups and geogs, but none of the building / population /
street-light / road-intersection counts the H3 and hotspot grids carry, so `beahiv202_building_counts`
was simply never produced. That was the original scope ("crime counts + geogs") and nothing had
extended it. Now that every point layer is tagged with `beahiv202_id`, the missing counts are the H3
query with one column swapped.

**What** — each counts module gained a `build_beahiv` / `beahiv_outputs` beside its existing
`build_hotspots`, and `beahiv_counts` wires the four in after building the crime counts, exactly as
`hotspot_counts` does for the hexes. The step now emits `beahiv202_{crime,streetlight,building,
population,road_intersection}_counts`.

The H3 and BEAHIV paths collapsed into **one** builder per module rather than two: both grids tag their
cell onto the feature, so counting is a group-and-count on `{grid}_id` either way, and the modules now
carry one id-column builder used twice plus the point-in-polygon one the hotspot hexes need because
they have no id column. Fewer code paths than before, not more.

**Design decisions**

- **Restricted to cells carrying crimes.** The BEAHIV grid *is* its crime cells — `beahiv202_geogs`
  covers no others, since the unit's cells come from `beahiv202_crime_counts` — so a count outside them
  would join to nothing. It also keeps the two grids comparable cell for cell, which is why the grid
  exists.
- **Population is filtered *after* allocation, not before.** Each building's share is normalised within
  its OA, so filtering the buildings going in would redistribute an OA's whole population across only
  those in crime cells and inflate them. Allocating over every building and then keeping the crime
  cells leaves each cell's figure untouched.
- **Road intersections stay an expression, not a column.** That layer is derived from the road network
  rather than extracted as a point layer, so it carries no cell id; the H3 path computes one via the h3
  extension (needing WGS-84) and BEAHIV by arithmetic on the BNG point. The shared builder therefore
  takes a cell *expression*, which the three tagged layers satisfy with a bare column name.
- **A layer without the column is skipped, not fatal.** `beahiv.tagged()` gates on the column existing,
  so parquet extracted before it was added are passed over with no output until they are re-run —
  matching how `population_counts` already handles the buildings size columns.

**Follow-ups**

- The counts only appear once the extracts are re-run: `buildings` and `streetlights` need
  `beahiv202_id` before their BEAHIV counts build at all.

**Superseded, same day** — the crime-cell restriction above was reversed once it was clear the two
grids disagreed. See the entry below.

## One rule for both grids: a cell computed from the point is always counted

**Why** — the entry above restricted the BEAHIV counts to crime-carrying cells, but H3 only did that
for two of its four: `h3r9_streetlight_counts` and `h3r9_population_counts` covered every cell, while
`h3r9_building_counts` and `h3r9_road_intersection_counts` were restricted. The grids exist to be
compared, so counting different features on each defeats the point — and the split within H3 was
itself unexplained.

| counts | H3 before | BEAHIV before | both now |
| --- | --- | --- | --- |
| streetlight | all cells | crime cells | **all cells** |
| population | all cells | crime cells | **all cells** |
| building | crime cells | crime cells | **all cells** |
| road_intersection | crime cells | crime cells | **all cells** |

**What** — the restriction is gone from all four on both grids: every cell holding a feature is
counted. The rule is now statable in one line — *if the cell comes from a point-to-cell calculation,
count every cell* — which is what the shared `_build_on` in each counts module does. The hotspot hexes
keep their point-in-polygon placement, being the one grid with no cell id, and they were never
restricted anyway ("the hexes *are* the grid").

**Design decisions**

- **The restriction was solving nothing.** It read as "so the count grid lines up with the crime grid",
  but the cell comes from the feature's own coordinates — there is no join for the crime grid to bound,
  and no cell can be produced that the grid could not represent. It only ever removed rows.
- **`*_geogs` still covers the crime cells alone**, on both grids, because a unit's cells are taken
  from its crime counts. So a cell counted without crimes has no attributes to join to — the same on
  H3 and BEAHIV, which is the consistency that matters. Worth knowing when joining counts to geogs:
  those rows are outer-join misses by design, not gaps.
- **This widens two published H3 tables.** `h3r9_building_counts` and
  `h3r9_road_intersection_counts` gain rows for cells holding a feature but no crime; consumers
  aggregating over them will see larger totals. That is the point of the change rather than a side
  effect, but it is a change to existing outputs.

## A cell id per grid on every point layer, and one place that names grids

**Why** — the extracts tagged each feature with `h3_9_id` and nothing else, so anything wanting the
BEAHIV grid had to re-derive the cell, and the column disagreed with the `{grid}_{dataset}` convention
the relations had just adopted. Both grids should be joinable straight off a raw layer.

**What** — every point layer (buildings by centroid, schools, poi, naptan, food_outlets, streetlights,
cctv) now carries **`h3r9_id` and `beahiv202_id`**, minted by one helper —
`extract._common.cell_id_columns(con, lat, lon, bng_geom)` — so the seven layers cannot drift in how
they name or derive them. The BEAHIV encoder moved from `transform/beahiv.py` to `beahiv_grid.py`,
which both phases already share, and `register_udf` moved to a neutral `duckdb_udf` module so the
extract phase does not have to import from the transform to use it.

`grids.py` is new and sits above both phases: it owns `h3_key()`, `relation()` and `id_column()`, so a
relation (`h3r9_crime_counts`) and a column (`h3r9_id`) are derived from the same key rather than
spelled out. `transform/base` re-exports them, so the steps still read as before.

A grid key is now a single token with no internal `_` — **`beahiv202`, not `beahiv_202`**, matching
`h3r9` — so exactly one underscore separates the grid from what follows it and a key stays greppable as
one word. That renames the extract module (`extract/beahiv202.py`), its dataset and parquet, and every
`beahiv202_*` relation.

**Design decisions**

- **One helper rather than seven copies of the SQL.** The layers already differ in how they reach a point
  (a footprint centroid, a WGS-84 geometry, an Easting/Northing pair), so the helper takes the lat/lon
  and BNG expressions and emits both columns; each extractor supplies only what is genuinely local to
  it. It registers the BEAHIV UDF as a side effect, mirroring `crime_locations.placed_in`, so a caller
  needs one import and cannot forget the registration.
- **The BNG point must be the same point as the lat/lon.** The H3 id comes from WGS-84 (what the h3
  extension takes) and the BEAHIV id by arithmetic on BNG (pyproj segfaults DuckDB's worker threads),
  so the two ids are derived from different expressions and would silently disagree if a caller passed
  a footprint to one and its centroid to the other. Documented on the helper, and the buildings
  extractor passes the centroid to both.
- **Verified against beahiv's own geometry, not the encoder.** A test asserts the tagged cell
  *contains* the feature (`cell_polygon(beahiv202_id).contains(point)`) — checking the encoder against
  itself would pass even if the whole grid were misaligned. The Overture-backed extract tests
  (poi/streetlights/cctv) exercise the columns end to end against live data.

**Follow-ups**

- The extracts must be re-run for the new column to exist on disk; every existing extract parquet still
  has `h3_9_id` and no BEAHIV id.
- `retail_centres` carries an unrelated `h3_count` column that predates all of this.

## One spatial join for the ONS counts (~180 min → 5s), and a `{grid}_{dataset}` naming convention

**Why** — `data transform --grid h3` ran for over two hours without producing a single parquet. Live
inspection of the process (6h35m CPU on 4 threads, 15.5 GB resident against a 15 GB cap, 4 GB spilled,
no extract parquet still open) put it inside `crime_counts`, which everything in the H3 chain waits on.
The step was rebuilding because `crime_counts_pfa24cd.parquet` was missing — collateral from the
`pfa23cd` → `pfa24cd` rename — and five of its six relations were point-in-polygon passes over 17.5M
crimes that have nothing to do with H3.

Measured, per point, over 20k points:

| layer | polygons | µs/point | 17.5M crimes |
| --- | --- | --- | --- |
| `police_force_areas` | 44 | **592.9** | **≈173 min** |
| `local_authority_districts` | 361 | 10.9 | 3.2 min |
| `msoa_2021` | 7,264 | 2.9 | 0.8 min |
| `lsoa_2021` | 35,672 | 2.8 | 0.8 min |
| `output_areas_2021` | 188,880 | 2.0 | 0.6 min |

Fewer, bigger polygons is the pathological case: 44 forces tile the country in "full extent" geometry
(~1 MB of vertices each), so the RTree prunes almost nothing and GEOS runs exact containment against a
million-vertex polygon every time. One layer was ~173 of the ~179 minutes.

**What** — `geography_counts`, a new step on a new `ons` grid family, taking the five
`{key}_crime_counts` out of `crime_counts` (which is now H3 only, and join-free: `h3_latlng_to_cell` is
arithmetic on the coordinates). It does **one** spatial join, at the finest layer:

- `crime_locations` — police.uk snaps crimes to a fixed point set, so 17.5M crimes sit on **743,990**
  distinct coordinates (23.5 each). Placing a location once and joining the answer back is the same
  result for ~4% of the work; the equi-join is on the source columns copied verbatim, so it is exact
  (asserted: 17,485,799 rows join, exactly the filtered input).
- `ons_hierarchy` — OA → LSOA → MSOA → LAD → PFA, built once from representative points
  (`ST_PointOnSurface`, which a centroid cannot replace on a concave polygon). 188,880 OAs resolve
  through every level, 0 unresolved, in 0.9s.
- a fallback: ~41k locations sit outside the OA layer while still inside a LAD, and funnelling
  everything through the OA would silently drop them. They turned out to be **Northern Ireland** —
  PSNI is on police.uk, and NI has neither an output area nor a police force area in the E&W layers.
  They are placed directly at the LAD level (41k points, not 744k) and their force follows from that.

The same trick fixes `geo_lookups`, where every grid was intersecting its cells against those same 44
polygons: a cell's force is now read off its LAD (`DERIVED_FROM`).

**Verified on the full extract** (17,485,799 countable crimes), not just the fixtures:

- all five geography counts build in **5.2s** (18.6s with the NI fallback), against ~180 minutes;
- `crime_counts_lad24cd` totals **17,483,713** — identical to a direct point-in-polygon;
- per location, derived vs direct: LSOA 3 differ of 702,757, MSOA 0, **PFA 112 of 702,760** (0.016%),
  all boundary-generalisation slivers between the statistical layers and the full-extent PFA layer;
- roll-ups conserve exactly at every level.

**Also: relation names now follow `{grid}_{dataset}`.** The counts trailed the unit
(`building_counts_hotspots`) while the lookups and geogs led with it (`hotspots_geogs`), so a directory
listing interleaved the families. Everything is now minted through `transform.base.relation()`:
`hotspots_building_counts`, `h3r9_crime_counts`, `beahiv_202_geogs`. The H3 key is `h3r9`, not `h3_9`,
so exactly one `_` separates grid from dataset.

**Design decisions**

- **Join the finest layer, derive the rest.** The instinct is to join the layer you want, but the
  cheapest layer is the one with the *most* polygons, and everything coarser is a lookup from it. The
  hierarchy is a property of the geographies, so this is not an approximation — the 112 PFA
  disagreements are the two layers' coastlines being generalised differently, and the derived answer is
  arguably the better one (a crime's force then agrees with its OA rather than contradicting it).
- **Dedupe rather than sample.** 23.5 crimes per point is a property of how police.uk anonymises, not
  of this dataset, so it will hold for every future extract.
- **`ons` as a fourth `Grid`.** The geography counts are not a grid, but they are a self-contained
  family with the same relation shape, and making them one means `--grid h3` no longer drags five
  point-in-polygon passes along with it. `ons_hierarchy` / `crime_locations` are `ensure()` helpers, not
  steps, so no cross-family `depends_on` is needed and the `--grid` filter stays safe.
- **`lsoa_code` was the obvious shortcut, and it is a trap.** police.uk ships an LSOA code per crime, so
  `crime_counts_lsoa21cd` looked like a `GROUP BY`. But 415,137 countable crimes (2.37%) carry no code
  at all, and the archive being 2023-07..2026-06 is the only reason every code is a valid 2021 one —
  extend it back past 2022 and retired 2011 codes appear, which no boundary layer contains. The spatial
  join is vintage-proof; the column is not.

**Follow-ups**

- Every transform parquet is renamed, so **all existing ones are orphaned** — locally and in the
  `phase2` container, which `sync` never deletes — and consumers joining `crime_counts_h3_9` or
  `building_counts_hotspots` must be updated. They need deleting by hand.
- The extract layers still carry an `h3_9_id` **column** (buildings, schools, streetlights, poi). It is
  the same inconsistency one level down, but renaming it means re-running those extracts.
- `crime_counts.build_hotspots` still places all 17.5M crimes rather than the 744k locations; the
  hotspot hexes are small polygons so it is not pathological, but it is the same free 23.5x.
- `_import_datasets` still imports and geometry-indexes all 25 extract datasets whatever `--grid` says;
  8 are read by no transform step at all.

## The per-geography lookups stop being published tables

**Why** — `{unit}_{key}_lookup` (five per grid: `h3_9_lad24cd_lookup`, `hotspots_oa21cd_lookup`, …) is
one row per cell carrying one ONS code, and `{unit}_geogs` carries every one of those codes as a column
over the same cells. Fifteen parquet of duplicated data, and two places to look up a cell's LSOA — with
nothing saying which is authoritative.

**What** — `geo_lookups.build_unit` still builds the lookups (`geogs` joins them), but as tables in the
transform's in-memory DuckDB rather than views, and the step declares no `outputs`, so none of them is
written to parquet or catalogued. `geo_lookups.unit_outputs` is deleted and `hotspot_lookups` /
`beahiv_lookups` drop it from their `outputs` — they still publish their overlap and retail lookups.

Verified before removing, on the built parquet in the data dir: for all five geographies on the
hotspots grid, `hotspots_{key}_lookup` and `hotspots_geogs` agree exactly — 4,433 cells each way, zero
rows on either side of an anti-join, zero differing codes. `*_geogs` takes its cell set from the
`lad24cd` lookup, so the check that matters is whether another key's lookup covers cells LAD's does
not: on both national grids (`h3_9`, 234,921 cells; `beahiv_202`, 221,428) every E&W-only lookup is a
strict subset — 0 cells outside. Nothing is lost. A regression test asserts the same equivalence on the
fixture grid, so this stays true.

**Design decisions**

- **Tables, not views.** Keeping them as views would have removed the second copy just as well, but
  then `geogs`' single query re-runs all five max-overlap joins inline — five spatial joins in one
  query, on a phase already tuned down to `threads = 4` to stay out of the OOM killer. Materialising in
  memory keeps the peak where it is today and actually saves a pass: a cold build used to run each
  join twice (once to write the parquet, once when `geogs` read the view).
- **Staleness had to be re-declared.** A step with no `outputs` has no parquet and therefore no mtime,
  so the chain "boundaries refreshed → `geo_lookups` rebuilt → `geogs` rebuilt" lost its middle link.
  `geogs` now lists the boundary tables in `extract_inputs` and `crime_counts` in `depends_on`
  directly (`beahiv_geogs` likewise on `beahiv_counts`; `hotspot_geogs` on `hotspots` + boundaries) —
  which is honest, since with the lookups unmaterialised those *are* the inputs it reads through.
- **The overlap lookups stay published.** They look like the same case but are not: `*_geogs` keeps
  only `{prefix}_ids` and one aggregate (MAX area / SUM length), so the per-feature overlap areas and
  the descriptive columns (greenspace function, road type, school name) exist only in the lookup.

**Follow-ups**

- `{unit}_retail_centre_lookup` *is* fully reproduced by `*_geogs` (`retail_centre_id` +
  `retail_centre_distance`, one row per cell) — the same argument applies, but it was outside the ask.
- The already-built lookup parquet are now orphaned: they stay in `data_dir()/transform` (and in the
  `phase2` container, which `sync` never deletes) and, since no step claims them any more, `data index`
  will catalogue them with a blank description. They need deleting by hand — the same clean-up the
  `pfa23cd` → `pfa24cd` rename left behind.

## `--grid` replaces `--resolutions`; the `load` phase is removed

**Why** — two things had outlived their design. `--resolutions` dated from when the H3 grid was built
at several resolutions; `H3_RESOLUTIONS` has been `[9]` since r8/10/11 were dropped, so the flag was a
knob that could only be set to its default, while the choice a run actually wants to make — *which of
the three grids to build* — could not be expressed at all. Rebuilding just the BEAHIV grid meant
rebuilding the H3 and hotspot families with it. And the `load` phase bundled the parquet into a
single-file DuckDB that nothing consumes: consumers query the parquet directly, locally or from the
blob container.

**What** — the `resolutions` parameter is gone from the transform contract: steps are now
`build(con, replace)` / `outputs(con)`, and the H3 steps read `H3_RESOLUTIONS` directly. In its place
each `TransformStep` declares a `grid` — `Grid.H3` / `Grid.HO` / `Grid.BEAHIV` — and `build_pipeline`
filters the registry by the requested families, so `data transform --grid beahiv` (repeatable, all
three by default) builds one grid and leaves the others' parquet untouched. `_validate` now rejects a
`depends_on` that crosses families, which is what makes the filter safe.

The `load` command, `assemble`, `run_load`, `_minimal_tables` and the `DEFAULT_FEATURE_TABLES` /
`DEFAULT_TRANSFORM_TABLES` sets are deleted, with their tests; `build` is now extract + transform. The
pipeline is two phases, and `index.parquet` (which catalogues what is on disk, not what was just built)
no longer takes a resolution list either.

**Design decisions**

- **Filter the registry, don't parameterise the steps.** The alternative was to pass the selected
  grids down to every `build` and let each step decide — the shape `resolutions` had. That repeats the
  same guard in fifteen modules and leaves each step free to ignore it; a `grid` field plus one filter
  in `build_pipeline` puts the decision in the registry, where `depends_on` and `outputs` already live.
- **Grid families must be self-contained, and that is now enforced.** Filtering assumes no step reads
  another family's relations. That was already true (the hotspot and BEAHIV steps read the extract
  tables and their own counts), but nothing said so, so a later cross-family `depends_on` would have
  turned a `--grid` subset into a build against relations that were never created. The import-time
  check in `_validate` makes it a rule rather than a coincidence.
- **Resolution is not a run-time knob.** Keeping `resolutions` threaded through as a library parameter
  defaulting to `H3_RESOLUTIONS` would have made a smaller diff, but it leaves a parameter no CLI
  exposes and no caller varies. A resolution is a property of the H3 gridding; if a second resolution
  is ever wanted, `H3_RESOLUTIONS` is the one place to add it and every H3 step follows.
- **`--grid ho`, not `--grid hotspots`.** The flag names the *family* (Home Office), not the relation
  prefix. The unit key stays `hotspots`, so the table names and the `local_only` rule are unchanged.
- **Delete `load` rather than deprecate it.** It has been documented as "optional — not currently
  used" for some time; keeping a deprecated command means keeping its code, its tests and its share of
  the docs current for a feature nobody runs. It is recoverable from git if a single-file bundle is
  ever wanted again.

**Follow-ups**

- `crime_counts` still builds the per-ONS-geography counts (`crime_counts_pfa24cd`, …) alongside the
  H3 counts, so they belong to the `h3` family: `--grid beahiv` alone does not refresh them. They are
  not a grid at all; if that becomes awkward, they want a step (and possibly a family) of their own.
- `data transform --grid` does not prune: narrowing a run leaves the other families' parquet in place,
  stale rather than removed. That is the intended behaviour (the parquet are a cache), but a
  `--grid`-aware `sync` cannot tell a stale family from a current one.

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

Also folded in: **`pfa23cd` → `pfa24cd`**. The previous entry's PR switched the PFA layer from Dec 2023
BGC to Dec 2024 BFE (`PFA23CD` → `PFA24CD` in `config/data_sources.json`) but left the
`GEOGRAPHY_MAPPINGS` key — and the `boundaries` description — naming the 2023 vintage, so `pfa23cd` has
been labelling 2024 codes ever since. Unrelated to this grid, but this change *mints* new relations
from that key (`beahiv_202_pfa23cd_lookup`, `beahiv_202_geogs.pfa23cd`), and shipping brand-new tables
already carrying the wrong vintage — then renaming them twice — is worse than correcting it here. It
renames `crime_counts_pfa23cd`, `h3_9_pfa23cd_lookup`, `hotspots_pfa23cd_lookup` and the `pfa23cd`
column of all three `*_geogs`, so **consumers joining on `pfa23cd` must be updated** and the
correspondingly-named parquet in the data dir are orphaned.

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
- *The cell geometry comes from beahiv, whole.* DuckDB has no BEAHIV cell function, so the geometry
  has to come from Python. `beahiv_cell_polygon` hands a whole DuckDB vector of ids to beahiv's
  `cell_polygons` and returns WKB for `ST_GeomFromWKB` — vectorised on both sides, no per-row Python,
  and no hexagon constructed in this repo. WKB rather than WKT: shorter, and exact, so nothing is
  lost to decimal rounding in transit. A test asserts the polygon reaching DuckDB is vertex-for-vertex
  `cell_polygon`'s, which catches a wrong CRS, a swapped x/y or a misaligned return vector; on the
  full 221,429-cell grid, zero polygons differ from `cell_polygons`.

  An earlier version instead returned only the cell *centre* and rebuilt the hexagon in SQL from six
  constant vertex offsets, which is ~50x cheaper per evaluation (0.36s vs 19.0s over the full grid,
  and the lookups are views, so `cells` is evaluated ~10 times in a `geogs` build — roughly +190s on
  an 877s build). It was rejected anyway: it generated SQL containing literals like
  `ctr.y + 2.4737865342776535e-14`, and it put this repo in the business of constructing hexagons that
  beahiv already knows how to construct. The cost is real but it buys back the most opaque code in the
  change; if the ~20% ever matters, materialising the polygons once into a `beahiv_202_cells` table
  gets it back without reintroducing the generated SQL (17.5s once, then 0.01s a read).
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
