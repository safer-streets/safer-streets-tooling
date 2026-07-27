"""``{grid}_{key}_lookup`` — each cell mapped to one ONS geography code, for every grid."""

import duckdb

from safer_streets_tooling.transform.base import TransformStep, create_clause
from safer_streets_tooling.transform.grids import grids

# short code -> boundary table name (the tables created by ons_boundaries.load_all).
# lad24 is listed for full-UK coverage; it is used as the base for {grid}_geogs.
GEOGRAPHY_MAPPINGS = {
    "pfa23cd": "police_force_areas",
    "lad24cd": "local_authority_districts",
    "msoa21cd": "msoa_2021",
    "lsoa21cd": "lsoa_2021",
    "oa21cd": "output_areas_2021",
}


def build(con: duckdb.DuckDBPyConnection, resolutions: list[int], replace: bool) -> None:
    """Create ``{grid}_{key}_lookup`` views mapping each cell to one ONS geography code.

    The cell boundary (in BNG — see :mod:`.grids` for how each grid gets there) is intersected with
    each boundary table. A cell may straddle several boundaries, so it is assigned to the one it
    overlaps most, guaranteeing a single row per cell.
    """
    for grid in grids(con, resolutions):
        for key, table in GEOGRAPHY_MAPPINGS.items():
            con.execute(f"""
                {create_clause("VIEW", f"{grid.key}_{key}_lookup", replace=replace)} AS
                SELECT c.spatial_id, b.spatial_id AS {key}
                FROM {grid.cells} c
                JOIN {table} b ON ST_Intersects(c.cell_geom, b.geom)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY c.spatial_id
                    ORDER BY ST_Area(ST_Intersection(c.cell_geom, b.geom)) DESC
                ) = 1;
            """)


def outputs(con: duckdb.DuckDBPyConnection, resolutions: list[int]) -> list[str]:
    return [f"{grid.key}_{key}_lookup" for grid in grids(con, resolutions) for key in GEOGRAPHY_MAPPINGS]


STEP = TransformStep(
    name="geo_lookups",
    build=build,
    outputs=outputs,
    description="Per-cell lookup mapping each H3 / BEAHIV cell to one ONS geography code (max-overlap).",
    depends_on=("crime_counts", "beahiv_counts"),
    extract_inputs=tuple(GEOGRAPHY_MAPPINGS.values()),
)
