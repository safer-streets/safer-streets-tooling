"""``hotspots_{key}_lookup`` — each hotspot hex mapped to one ONS geography code (in memory only).

The hotspot counterpart of :mod:`.geo_lookups`: the same :func:`~.geo_lookups.build_unit` over
:data:`~safer_streets_tooling.transform.hotspots.HOTSPOT_UNIT`. It declares no ``outputs``, so it always
builds, for the reason given in :mod:`.beahiv_geo_lookups`.
"""

import duckdb

from safer_streets_tooling.transform import geo_lookups, hotspots
from safer_streets_tooling.transform.base import Grid, TransformStep


def build(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Build every ``hotspots_{key}_lookup``."""
    if not hotspots.available(con):
        return
    geo_lookups.build_unit(con, hotspots.HOTSPOT_UNIT, replace)


def outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    return []


STEP = TransformStep(
    name="hotspot_geo_lookups",
    build=build,
    outputs=outputs,
    grid=Grid.HO,
    description="Per-hex ONS geography codes (max-overlap) on the hotspot hexes; in memory only, folded into hotspots_geogs.",
    extract_inputs=("hotspots", *geo_lookups.STEP.extract_inputs),
)
