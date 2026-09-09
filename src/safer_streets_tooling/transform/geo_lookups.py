"""``{unit}_{key}_lookup`` — each spatial unit (H3 cell / hotspot hex) mapped to one ONS geography code.

These are **build intermediates, not published tables**: ``{unit}_geogs`` is built from them and carries
every code as a column over the same cells, so a per-key parquet alongside it would be the same data
twice (and two places to look for a cell's LSOA). They are materialised as tables in the transform's
in-memory DuckDB — ``geogs`` joins all five, so paying for each max-overlap join once is cheaper than
letting it re-run inside that query — and the step declares no ``outputs``, so nothing reaches disk.
"""

import duckdb

from safer_streets_tooling.transform import ons_hierarchy
from safer_streets_tooling.transform.base import (
    H3_RESOLUTIONS,
    Grid,
    SpatialUnit,
    TransformStep,
    create_clause,
    h3_unit,
)

# the ONS geography registry (short code -> boundary table) lives with the hierarchy that relates the
# geographies to each other; it is re-exported here, where the rest of the transform reads it from.
from safer_streets_tooling.transform.ons_hierarchy import GEOGRAPHY_MAPPINGS

__all__ = ["GEOGRAPHY_MAPPINGS", "STEP", "build", "build_unit", "outputs"]

# code -> the code it is read off instead of being intersected directly. Only the police force areas:
# 44 polygons tiling the country in "full extent" geometry, which an RTree cannot prune and GEOS then
# tests exactly against ~1 MB of vertices — measured at 593 us per point against 2.0 us for the 188,880
# output areas. A force is a union of whole LADs (no LAD in the layer straddles two), so a cell's force
# is its LAD's force. The other four stay direct: their polygons are small enough to be cheap, and
# max-overlap is the honest answer for a cell that straddles them.
DERIVED_FROM = {"pfa24cd": "lad24cd"}


def build_unit(con: duckdb.DuckDBPyConnection, unit: SpatialUnit, replace: bool) -> None:
    """Create ``{unit.key}_{key}_lookup`` tables mapping each of ``unit``'s cells to one ONS geography code.

    The cell boundary (BNG) is intersected with each boundary table. A cell may straddle several
    boundaries, so it is assigned to the one it overlaps most, guaranteeing a single row per cell.
    :data:`DERIVED_FROM` names the exception, read off another lookup rather than joined.
    In-memory only — ``geogs`` folds them into ``{unit.key}_geogs`` and they are not written out.
    """
    ons_hierarchy.ensure(con)
    for key, table in GEOGRAPHY_MAPPINGS.items():
        if key in DERIVED_FROM:
            continue
        con.execute(f"""
            {create_clause("TABLE", f"{unit.key}_{key}_lookup", replace=replace)} AS
            SELECT c.spatial_id, b.spatial_id AS {key}
            FROM ({unit.cells}) c
            JOIN {table} b ON ST_Intersects(c.cell_geom, b.geom)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY c.spatial_id
                ORDER BY ST_Area(ST_Intersection(c.cell_geom, b.geom)) DESC
            ) = 1;
        """)
    for key, parent in DERIVED_FROM.items():
        con.execute(f"""
            {create_clause("TABLE", f"{unit.key}_{key}_lookup", replace=replace)} AS
            SELECT p.spatial_id, h.{key}
            FROM {unit.key}_{parent}_lookup p
            JOIN (SELECT DISTINCT {parent}, {key} FROM {ons_hierarchy.TABLE} WHERE {key} IS NOT NULL) h
              USING ({parent});
        """)


def build(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    for res in H3_RESOLUTIONS:
        build_unit(con, h3_unit(res), replace)


def outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    """None: the lookups are in-memory intermediates that ``geogs`` publishes as its own columns.

    A step with no outputs is never cached, so this one rebuilds on every run — which is what we want,
    since its consumer can only read the tables from the catalog, not from parquet.
    """
    return []


STEP = TransformStep(
    name="geo_lookups",
    build=build,
    outputs=outputs,
    grid=Grid.H3,
    description="Per-cell lookup mapping each H3 cell to one ONS geography code (max-overlap); folded into h3_{res}_geogs.",
    depends_on=("crime_counts",),
    extract_inputs=tuple(GEOGRAPHY_MAPPINGS.values()),
)
