"""``beahiv_202_geogs`` — one row per BEAHIV cell: ONS codes + overlap id lists + nearest retail centre.

The BEAHIV counterpart of ``h3_{res}_geogs``: same columns, same scope (see
:mod:`safer_streets_tooling.transform.geogs`), built by the same query over the BEAHIV lookups — which
is what makes the two griddings directly comparable. Its ``cell_area`` is the analytic hexagon area,
a constant, because the grid is equal-area in EPSG:27700.
"""

import duckdb

from safer_streets_tooling.transform import beahiv, geo_lookups, geogs
from safer_streets_tooling.transform.base import Grid, TransformStep


def build(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Build ``beahiv_202_geogs``."""
    if not beahiv.available(con):
        return
    beahiv.register_udfs(con)  # the lookups are views, so their cell-polygon UDF calls run here
    geogs.build_unit(con, beahiv.BEAHIV_UNIT, replace)


def outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [f"{beahiv.BEAHIV_UNIT.key}_geogs"] if beahiv.available(con) else []


STEP = TransformStep(
    name="beahiv_geogs",
    build=build,
    outputs=outputs,
    grid=Grid.BEAHIV,
    description="One row per BEAHIV cell: ONS codes, overlap id lists + measures, cell_area, nearest retail centre.",
    # beahiv_counts and the boundary layers are read through the geography lookups, which publish no
    # parquet, so this step needs their mtimes itself to notice a refreshed input (see :mod:`.geogs`)
    depends_on=("beahiv_counts", "beahiv_lookups"),
    extract_inputs=geo_lookups.STEP.extract_inputs,
)
