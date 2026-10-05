"""``beahiv202_retail_centre_lookup`` — each BEAHIV cell's nearest retail centre. Deprecated, not built by default.

The BEAHIV counterpart of :mod:`.retail_centre_lookups` (see there for why it is deprecated): the same
:func:`~.retail_centre_lookups.build_unit` over :data:`~safer_streets_tooling.transform.beahiv.BEAHIV_UNIT`.
It is a step of its own, not part of ``beahiv_lookups``, so that it can be left out of a build.
"""

import duckdb

from safer_streets_tooling.transform import beahiv, retail_centre_lookups
from safer_streets_tooling.transform.base import Grid, TransformStep


def build(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Build ``beahiv202_retail_centre_lookup``."""
    if not beahiv.available(con):
        return
    beahiv.register_udfs(con)  # BEAHIV_UNIT.cells calls the cell-polygon UDF
    retail_centre_lookups.build_unit(con, beahiv.BEAHIV_UNIT, replace)


def outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    return retail_centre_lookups.unit_outputs(con, beahiv.BEAHIV_UNIT) if beahiv.available(con) else []


STEP = TransformStep(
    name="beahiv_retail_centre_lookups",
    build=build,
    outputs=outputs,
    grid=Grid.BEAHIV,
    description="Deprecated, not built by default: per-cell lookup of each BEAHIV cell's nearest retail centre (within 2km) + distance.",
    depends_on=("beahiv_counts",),
    extract_inputs=retail_centre_lookups.STEP.extract_inputs,
    default=False,
)
