"""``h3_{res}_{key}_lookup`` — each H3 cell mapped to one ONS geography code."""

import duckdb

from safer_streets_tooling.transform.base import TransformStep, create_clause

# short code -> boundary table name (the tables created by ons_boundaries.load_all).
# lad24 is listed for full-UK coverage; it is used as the base for h3_*_geogs.
GEOGRAPHY_MAPPINGS = {
    "pfa23cd": "police_force_areas",
    "lad24cd": "local_authority_districts",
    "msoa21cd": "msoa_2021",
    "lsoa21cd": "lsoa_2021",
    "oa21cd": "output_areas_2021",
}


def build(con: duckdb.DuckDBPyConnection, resolutions: list[int], replace: bool) -> None:
    """Create ``h3_{res}_{key}_lookup`` views mapping each H3 cell to one ONS geography code.

    The H3 cell boundary (WGS-84) is transformed to BNG and intersected with each boundary
    table. A cell may straddle several boundaries, so it is assigned to the one it overlaps
    most, guaranteeing a single row per cell.
    """
    for res in resolutions:
        for key, table in GEOGRAPHY_MAPPINGS.items():
            con.execute(f"""
                {create_clause("VIEW", f"h3_{res}_{key}_lookup", replace=replace)} AS
                SELECT c.spatial_id, b.spatial_id AS {key}
                FROM (
                    SELECT DISTINCT
                        spatial_id,
                        ST_Transform(
                            ST_GeomFromText(h3_cell_to_boundary_wkt(spatial_id)),
                            'EPSG:4326', 'EPSG:27700', always_xy := true
                        ) AS cell_geom
                    FROM crime_counts_h3_{res}
                ) c
                JOIN {table} b ON ST_Intersects(c.cell_geom, b.geom)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY c.spatial_id
                    ORDER BY ST_Area(ST_Intersection(c.cell_geom, b.geom)) DESC
                ) = 1;
            """)


def outputs(con: duckdb.DuckDBPyConnection, resolutions: list[int]) -> list[str]:
    return [f"h3_{res}_{key}_lookup" for res in resolutions for key in GEOGRAPHY_MAPPINGS]


STEP = TransformStep(
    name="geo_lookups",
    build=build,
    outputs=outputs,
    depends_on=("crime_counts",),
    extract_inputs=tuple(GEOGRAPHY_MAPPINGS.values()),
)
