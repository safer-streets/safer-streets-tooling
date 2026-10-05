"""``hotspots_retail_centre_lookup`` — each hotspot hex's nearest retail centre. Deprecated, not built by default.

The hotspot counterpart of :mod:`.retail_centre_lookups` (see there for why it is deprecated): the same
:func:`~.retail_centre_lookups.build_unit` over :data:`~safer_streets_tooling.transform.hotspots.HOTSPOT_UNIT`.
It is a step of its own, not part of ``hotspot_lookups``, so that it can be left out of a build.
"""

import duckdb

from safer_streets_tooling.transform import hotspots, retail_centre_lookups
from safer_streets_tooling.transform.base import Grid, TransformStep


def build(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Build ``hotspots_retail_centre_lookup``."""
    if not hotspots.available(con):
        return
    retail_centre_lookups.build_unit(con, hotspots.HOTSPOT_UNIT, replace)


def outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    return retail_centre_lookups.unit_outputs(con, hotspots.HOTSPOT_UNIT) if hotspots.available(con) else []


STEP = TransformStep(
    name="hotspot_retail_centre_lookups",
    build=build,
    outputs=outputs,
    grid=Grid.HO,
    description="Deprecated, not built by default: per-hex lookup of each hotspot hex's nearest retail centre (within 2km) + distance.",
    extract_inputs=("hotspots", *retail_centre_lookups.STEP.extract_inputs),
    default=False,
)
