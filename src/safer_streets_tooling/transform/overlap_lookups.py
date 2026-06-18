"""``h3_{res}_{name}_lookup`` — every overlapping feature (greenspace, land cover, roads, school isochrones) per H3 cell."""

from dataclasses import dataclass

import duckdb

from safer_streets_tooling.transform.base import TransformStep, create_clause, table_exists


@dataclass(frozen=True)
class OverlapFeature:
    """A many-to-many geometry layer overlapped per H3 cell and folded into h3_geogs as a list."""

    table: str  # source table with a geometry column (may be absent → feature skipped)
    name: str  # view name h3_{res}_{name}_lookup
    id_col: str  # id column in the source table
    extra_col: str  # an extra descriptive column carried in the lookup view
    extra_alias: str  # alias for that extra column in the lookup view
    cte: str  # short CTE alias used when folding the list into h3_geogs
    id_alias: str = ""  # prefix for the {prefix}_id / {prefix}_ids columns (defaults to `name`)
    geom_col: str = "geom"  # geometry column overlapped against each cell (e.g. `isochrone` for schools)
    overlap_fn: str = "ST_Area"  # ST_Area for polygons, ST_Length for line layers (e.g. roads)
    overlap_alias: str = "overlap_area"  # name of the overlap-measure column in the lookup view
    # how h3_geogs folds the per-cell overlap measure into one value. MAX for area layers (polygons of
    # different types can overlap each other, so summing double-counts); SUM for line length (additive).
    agg_fn: str = "MAX"

    @property
    def prefix(self) -> str:
        return self.id_alias or self.name


# optional geometry layers folded into h3_geogs, each skipped if its table is absent. Loaded by
# data_pipeline: open_greenspace (OS Open Greenspace), land_cover (UKCEH LCM), road_network (OS Open Roads),
# schools (GIAS, overlapped by their walk-isochrone catchment rather than the point location).
OVERLAP_FEATURES: tuple[OverlapFeature, ...] = (
    OverlapFeature("open_greenspace", "greenspace", "id", "function", "function", "gs"),
    OverlapFeature("land_cover", "land_cover", "gid", "urban", "urban", "lc"),
    OverlapFeature(
        "open_roads",
        "road_network",
        "id",
        "road_function",
        "type",
        "rn",
        id_alias="road",
        overlap_fn="ST_Length",
        overlap_alias="overlap_length",
        agg_fn="SUM",
    ),
    OverlapFeature(
        "schools",
        "schools",
        "urn",
        "establishmentname",
        "school_name",
        "sc",
        id_alias="school",
        geom_col="isochrone",
    ),
)


def build(con: duckdb.DuckDBPyConnection, resolutions: list[int], replace: bool) -> None:
    """Create ``h3_{res}_{name}_lookup`` views: one row per (H3 cell, overlapping polygon).

    Unlike the single-code geography lookups, a cell keeps *every* feature it intersects, with
    the overlap measure (area for polygons, length for line layers). The overlapped geometry is the
    table's ``geom`` by default, but schools use their walk-isochrone catchment instead. Each feature
    is skipped if its source table is absent (e.g. the greenspace, land-cover, road or schools load
    was skipped).
    """
    for f in OVERLAP_FEATURES:
        if not table_exists(con, f.table):
            continue
        for res in resolutions:
            con.execute(f"""
                {create_clause("VIEW", f"h3_{res}_{f.name}_lookup", replace=replace)} AS
                SELECT
                    c.spatial_id,
                    s.{f.id_col} AS {f.prefix}_id,
                    s.{f.extra_col} AS {f.extra_alias},
                    {f.overlap_fn}(ST_Intersection(c.cell_geom, s.{f.geom_col})) AS {f.overlap_alias}
                FROM (
                    SELECT DISTINCT
                        spatial_id,
                        ST_Transform(
                            ST_GeomFromText(h3_cell_to_boundary_wkt(spatial_id)),
                            'EPSG:4326', 'EPSG:27700', always_xy := true
                        ) AS cell_geom
                    FROM crime_counts_h3_{res}
                ) c
                JOIN {f.table} s ON ST_Intersects(c.cell_geom, s.{f.geom_col});
            """)


def outputs(con: duckdb.DuckDBPyConnection, resolutions: list[int]) -> list[str]:
    # only features whose source table is present are built (matching build)
    return [f"h3_{res}_{f.name}_lookup" for f in OVERLAP_FEATURES if table_exists(con, f.table) for res in resolutions]


STEP = TransformStep(
    name="overlap_lookups",
    build=build,
    outputs=outputs,
    depends_on=("crime_counts",),
    extract_inputs=tuple(f.table for f in OVERLAP_FEATURES),
)
