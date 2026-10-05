"""``beahiv202_{name}_lookup`` — the overlap lookups, built on the BEAHIV 202m hex grid.

The overlap layers are the H3 ones with a different set of cells, so this step calls
:func:`~.overlap_lookups.build_unit` with :data:`~safer_streets_tooling.transform.beahiv.BEAHIV_UNIT`. Like
the H3 units and unlike the hotspot hexes, the cells come from the crime counts, so this waits on
``beahiv_counts``. The geography lookups are a step of their own, :mod:`.beahiv_geo_lookups`, because they
are never cached and this step is.
"""

import duckdb

from safer_streets_tooling.transform import beahiv, overlap_lookups
from safer_streets_tooling.transform.base import Grid, TransformStep


def build(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Build every BEAHIV overlap lookup."""
    if not beahiv.available(con):
        return
    beahiv.register_udfs(con)  # BEAHIV_UNIT.cells calls the cell-polygon UDF
    overlap_lookups.build_unit(con, beahiv.BEAHIV_UNIT, replace)


def outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    return overlap_lookups.unit_outputs(con, beahiv.BEAHIV_UNIT) if beahiv.available(con) else []


STEP = TransformStep(
    name="beahiv_lookups",
    build=build,
    outputs=outputs,
    grid=Grid.BEAHIV,
    description="Per-cell lookups on the BEAHIV grid: every overlapping feature.",
    depends_on=("beahiv_counts",),
    extract_inputs=overlap_lookups.STEP.extract_inputs,
)
