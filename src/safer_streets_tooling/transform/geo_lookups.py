"""``{unit}_{key}_lookup`` — each spatial unit (H3 cell / hotspot hex) mapped to one ONS geography code."""

import duckdb

from safer_streets_tooling.transform.base import SpatialUnit, TransformStep, create_clause, h3_unit

# short code -> boundary table name (the tables created by ons_boundaries.load_all).
# lad24 is listed for full-UK coverage; it is used as the base for the *_geogs tables.
GEOGRAPHY_MAPPINGS = {
    "pfa23cd": "police_force_areas",
    "lad24cd": "local_authority_districts",
    "msoa21cd": "msoa_2021",
    "lsoa21cd": "lsoa_2021",
    "oa21cd": "output_areas_2021",
}


def build_unit(con: duckdb.DuckDBPyConnection, unit: SpatialUnit, replace: bool) -> None:
    """Create ``{unit.key}_{key}_lookup`` views mapping each of ``unit``'s cells to one ONS geography code.

    The cell boundary (BNG) is intersected with each boundary table. A cell may straddle several
    boundaries, so it is assigned to the one it overlaps most, guaranteeing a single row per cell.
    """
    for key, table in GEOGRAPHY_MAPPINGS.items():
        con.execute(f"""
            {create_clause("VIEW", f"{unit.key}_{key}_lookup", replace=replace)} AS
            SELECT c.spatial_id, b.spatial_id AS {key}
            FROM ({unit.cells}) c
            JOIN {table} b ON ST_Intersects(c.cell_geom, b.geom)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY c.spatial_id
                ORDER BY ST_Area(ST_Intersection(c.cell_geom, b.geom)) DESC
            ) = 1;
        """)


def unit_outputs(unit: SpatialUnit) -> list[str]:
    return [f"{unit.key}_{key}_lookup" for key in GEOGRAPHY_MAPPINGS]


def build(con: duckdb.DuckDBPyConnection, resolutions: list[int], replace: bool) -> None:
    for res in resolutions:
        build_unit(con, h3_unit(res), replace)


def outputs(con: duckdb.DuckDBPyConnection, resolutions: list[int]) -> list[str]:
    return [name for res in resolutions for name in unit_outputs(h3_unit(res))]


STEP = TransformStep(
    name="geo_lookups",
    build=build,
    outputs=outputs,
    description="Per-cell lookup mapping each H3 cell to one ONS geography code (max-overlap).",
    depends_on=("crime_counts",),
    extract_inputs=tuple(GEOGRAPHY_MAPPINGS.values()),
)
