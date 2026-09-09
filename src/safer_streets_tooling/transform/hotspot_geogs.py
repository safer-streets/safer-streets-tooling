"""``hotspots_geogs`` — one row per Home Office hotspot hex: ONS codes + overlap id lists + nearest retail centre.

The hotspot counterpart of ``h3_{res}_geogs``: same columns, same scope (see
:mod:`safer_streets_tooling.transform.geogs`), built by the same query over the hotspot lookups. Its
``cell_area`` is the hex polygon's own area in m² rather than an H3 cell's geodesic area.
"""

import duckdb

from safer_streets_tooling.transform import geo_lookups, geogs, hotspots
from safer_streets_tooling.transform.base import Grid, TransformStep


def build(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Build ``hotspots_geogs``."""
    if not hotspots.available(con):
        return
    geogs.build_unit(con, hotspots.HOTSPOT_UNIT, replace)


def outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [f"{hotspots.HOTSPOT_UNIT.key}_geogs"] if hotspots.available(con) else []


STEP = TransformStep(
    name="hotspot_geogs",
    build=build,
    outputs=outputs,
    grid=Grid.HO,
    description="One row per hotspot hex: ONS codes, overlap id lists + measures, cell_area, nearest retail centre.",
    depends_on=("hotspot_lookups",),
    # the hexes and the boundary layers are read through the geography lookups, which publish no
    # parquet, so this step needs their mtimes itself to notice a refreshed input (see :mod:`.geogs`)
    extract_inputs=("hotspots", *geo_lookups.STEP.extract_inputs),
)
