"""``hotspots_{name}_lookup`` — the overlap lookups, built on the Home Office hotspot hexes.

The overlap layers are the H3 ones with a different set of cells, so this step calls
:func:`~.overlap_lookups.build_unit` with :data:`~safer_streets_tooling.transform.hotspots.HOTSPOT_UNIT`.
Unlike the H3 units, the cells come from the hotspots table rather than from the crime counts, so this step
depends on no other transform step. The geography lookups are a step of their own,
:mod:`.hotspot_geo_lookups`, because they are never cached and this step is.
"""

import duckdb

from safer_streets_tooling.transform import hotspots, overlap_lookups
from safer_streets_tooling.transform.base import Grid, TransformStep


def build(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Build every hotspot overlap lookup."""
    if not hotspots.available(con):
        return
    overlap_lookups.build_unit(con, hotspots.HOTSPOT_UNIT, replace)


def outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    return overlap_lookups.unit_outputs(con, hotspots.HOTSPOT_UNIT) if hotspots.available(con) else []


STEP = TransformStep(
    name="hotspot_lookups",
    build=build,
    outputs=outputs,
    grid=Grid.HO,
    description="Per-hex lookups: every overlapping feature.",
    extract_inputs=("hotspots", *overlap_lookups.STEP.extract_inputs),
)
