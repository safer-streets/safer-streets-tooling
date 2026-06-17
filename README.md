# safer-streets-tooling

Data-build tooling for the safer-streets project. Builds the production DuckDB database
(crime + ONS boundaries + supplementary layers + H3 aggregations) from modular, per-dataset
GeoParquet intermediates. Depends on [`safer-streets-core`](../safer-streets-core) for the database
helpers, H3 transforms, the data-source catalogue, and the ONS boundary downloader.

## Pipeline

Three phases (extract → transform → load), driven by a dataset registry
(`safer_streets_tooling.extract.DATASETS`) and a transform-step registry
(`safer_streets_tooling.transform.STEPS`):

1. **extract** — each dataset is downloaded and preprocessed in its own in-memory DuckDB and dumped to
   a `<name>.parquet` GeoParquet file under `data_dir()/extract` (raw source files are cached under
   `data_dir()/raw`). Extractors run **concurrently** as
   nodes in an `AsyncPipeline`, respecting `depends_on` edges (e.g. `schools` waits for `open_roads`,
   `imd` for `local_authority_districts`). Each parquet is a durable per-dataset cache, so a single
   dataset can be refreshed without rebuilding everything.
2. **transform** — the extracted parquet are loaded into a throwaway in-memory DuckDB, geometry is
   indexed, and the H3 aggregation steps (`safer_streets_tooling.transform.STEPS`) run. The BTP-filtered
   `crime_counts_h3_*` are aggregated from `crime_data`, then every derived relation (those counts, the
   per-cell lookups and `h3_{res}_geogs`) is written out as its own parquet under `data_dir()/transform`
   — a durable cache, so the aggregations can be rebuilt without re-extracting.
3. **load** — every present parquet (extracted datasets + transform aggregations) is imported into a
   `<name>.staging.db`, geometry tables are repaired and RTree-indexed, and the staging file is
   atomically promoted over the live database. Consumers therefore only ever see a complete database.

### Extract & transform DAG

In **extract**, every dataset is an `AsyncNode` keyed by its name; `depends_on` are the edges. Nodes
with no incoming edge start immediately and run concurrently (each blocking extractor in a worker
thread); a dependent only starts once its dependencies have produced their parquet. In **transform**
(run during assemble, `safer_streets_tooling.transform`), each step is likewise an `AsyncNode` keyed by
its name with `depends_on` edges: the BTP-filtered `crime_counts_h3_N` are aggregated from `crime_data`;
every H3 cell is keyed off them, then given one ONS code per geography,
the overlapping greenspace / land-cover / road features, and its nearest retail centre — all folded
into `h3_N_geogs`. (For brevity the transform nodes collapse the per-resolution `N ∈ {8,9,10}`; the
geography / overlap / retail lookups all draw their cell set from `crime_counts_h3_N`.)

```mermaid
flowchart LR
    subgraph EXTRACT["Extract · one parquet per dataset"]
        direction LR
        crime_data
        police_force_areas
        local_authority_districts
        msoa_2021
        lsoa_2021
        output_areas_2021
        open_greenspace
        land_cover
        retail_centres
        open_roads
        poi
        schools
        imd_scores_pct
    end

    subgraph TRANSFORM["Transform · fast H3 lookup tables"]
        direction LR
        crime_counts_h3_8
        crime_counts_h3_9
        crime_counts_h3_10
        h3_geogs_lookup
        h3_greenspace_lookup
        h3_land_cover_lookup
        h3_road_network_lookup
        h3_retail_centres_lookup
        h3_8_geogs
        h3_9_geogs
        h3_10_geogs
    end

    %% extract edges
    open_roads --> schools
    local_authority_districts --> imd_scores_pct

    %% transform edges
    crime_data --> crime_counts_h3_8
    crime_data --> crime_counts_h3_9
    crime_data --> crime_counts_h3_10
    crime_counts_h3_8 --> h3_geogs_lookup
    crime_counts_h3_9 --> h3_geogs_lookup
    crime_counts_h3_10 --> h3_geogs_lookup
    police_force_areas --> h3_geogs_lookup
    local_authority_districts --> h3_geogs_lookup
    msoa_2021 --> h3_geogs_lookup
    lsoa_2021 --> h3_geogs_lookup
    output_areas_2021 --> h3_geogs_lookup
    open_greenspace --> h3_greenspace_lookup
    land_cover --> h3_land_cover_lookup
    open_roads --> h3_road_network_lookup
    retail_centres --> h3_retail_centres_lookup
    h3_geogs_lookup --> h3_8_geogs
    h3_greenspace_lookup --> h3_8_geogs
    h3_land_cover_lookup --> h3_8_geogs
    h3_road_network_lookup --> h3_8_geogs
    h3_retail_centres_lookup --> h3_8_geogs
    h3_geogs_lookup --> h3_9_geogs
    h3_greenspace_lookup --> h3_9_geogs
    h3_land_cover_lookup --> h3_9_geogs
    h3_road_network_lookup --> h3_9_geogs
    h3_retail_centres_lookup --> h3_9_geogs
    h3_geogs_lookup --> h3_10_geogs
    h3_greenspace_lookup --> h3_10_geogs
    h3_land_cover_lookup --> h3_10_geogs
    h3_road_network_lookup --> h3_10_geogs
    h3_retail_centres_lookup --> h3_10_geogs

    %% colour by phase, tuned for dark backgrounds (white text on saturated fills, light strokes)
    classDef extract fill:#1f6feb,stroke:#79c0ff,stroke-width:1px,color:#ffffff;
    classDef transform fill:#8957e5,stroke:#d2a8ff,stroke-width:1px,color:#ffffff;
    class crime_data,police_force_areas,local_authority_districts,msoa_2021,lsoa_2021,output_areas_2021,open_greenspace,land_cover,retail_centres,open_roads,poi,schools,imd_scores_pct extract;
    class crime_counts_h3_8,crime_counts_h3_9,crime_counts_h3_10,h3_8_geogs,h3_9_geogs,h3_10_geogs transform;
```

Each extract node writes `<name>.parquet`; the **transform** phase turns those into the H3
aggregation parquet, and **load** imports both sets into the live database.

Geometry is British National Grid (EPSG:27700) by convention; the DuckDB GeoParquet writer tags it
`OGC:CRS84`, which is stripped to a bare `GEOMETRY` on load (the coordinates are the contract).

## Usage

```bash
uv sync
data build                       # extract any missing parquet, then transform + load
data extract                     # (re)build only missing parquet intermediates
data extract --only schools      # refresh one dataset (reads open_roads.parquet from cache)
data extract --force-download    # re-fetch every source and rebuild
data transform                   # (re)build the H3 aggregation parquet from the extract parquet
data load                        # rebuild the DB from whatever parquet exist (extract + transform)
data assemble                    # transform + load in one step
```

## Adding a dataset

1. Write a module under `src/safer_streets_tooling/extract/` exposing a `DATASET = Dataset(...)`
   whose `extract(ctx)` writes `ctx.parquet(name)` (use `_common.write_geoparquet`).
2. Register it in `src/safer_streets_tooling/extract/__init__.py` (after any `depends_on`).
3. `data extract --only <name>` then `data assemble`.

## Adding a transform step

1. Write a module under `src/safer_streets_tooling/transform/` exposing a `STEP = TransformStep(...)`
   with a `build(con, resolutions, replace)`, an `outputs(con, resolutions)` listing the relations it
   produces, and the names of any steps it `depends_on`.
2. Register it in `src/safer_streets_tooling/transform/__init__.py` (after any `depends_on`).
3. `data transform` then `data load` (or `data assemble`).
