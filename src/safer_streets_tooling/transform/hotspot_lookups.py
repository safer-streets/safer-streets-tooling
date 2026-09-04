"""``hotspots_*_lookup`` — the per-cell lookups, built on the Home Office hotspot hexes.

The three lookup families (ONS geography code, overlapping feature layers, nearest retail centre) are
the H3 ones with a different set of cells, so this step just calls each module's ``build_unit`` with
:data:`~safer_streets_tooling.transform.hotspots.HOTSPOT_UNIT`. Unlike the H3 units, the cells come from
the hotspots table rather than from the crime counts, so this step depends on no other transform step.
"""

import duckdb

from safer_streets_tooling.transform import geo_lookups, hotspots, overlap_lookups, retail_centre_lookups
from safer_streets_tooling.transform.base import TransformStep

_MODULES = (geo_lookups, overlap_lookups, retail_centre_lookups)


def build(con: duckdb.DuckDBPyConnection, resolutions: list[int], replace: bool) -> None:
    """Build every hotspot lookup. ``resolutions`` is ignored — the hexes are their own grid."""
    if not hotspots.available(con):
        return
    for module in _MODULES:
        module.build_unit(con, hotspots.HOTSPOT_UNIT, replace)


def outputs(con: duckdb.DuckDBPyConnection, resolutions: list[int]) -> list[str]:
    if not hotspots.available(con):
        return []
    return [
        *geo_lookups.unit_outputs(hotspots.HOTSPOT_UNIT),
        *overlap_lookups.unit_outputs(con, hotspots.HOTSPOT_UNIT),
        *retail_centre_lookups.unit_outputs(con, hotspots.HOTSPOT_UNIT),
    ]


STEP = TransformStep(
    name="hotspot_lookups",
    build=build,
    outputs=outputs,
    description="Per-hex lookups: its ONS geography codes (max-overlap), every overlapping feature, and its nearest retail centre.",
    extract_inputs=(
        "hotspots",
        *geo_lookups.STEP.extract_inputs,
        *overlap_lookups.STEP.extract_inputs,
        *retail_centre_lookups.STEP.extract_inputs,
    ),
)
