"""``beahiv_202_*_lookup`` — the per-cell lookups, built on the BEAHIV 202m hex grid.

The three lookup families (ONS geography code, overlapping feature layers, nearest retail centre) are
the H3 ones with a different set of cells, so this step just calls each module's ``build_unit`` with
:data:`~safer_streets_tooling.transform.beahiv.BEAHIV_UNIT`. Like the H3 units and unlike the hotspot
hexes, the cells come from the crime counts, so this waits on ``beahiv_counts``.
"""

import duckdb

from safer_streets_tooling.transform import beahiv, geo_lookups, overlap_lookups, retail_centre_lookups
from safer_streets_tooling.transform.base import TransformStep

_MODULES = (geo_lookups, overlap_lookups, retail_centre_lookups)


def build(con: duckdb.DuckDBPyConnection, resolutions: list[int], replace: bool) -> None:
    """Build every BEAHIV lookup. ``resolutions`` is ignored — the hexes are their own grid."""
    if not beahiv.available(con):
        return
    beahiv.register_udfs(con)  # BEAHIV_UNIT.cells calls the centre UDF
    for module in _MODULES:
        module.build_unit(con, beahiv.BEAHIV_UNIT, replace)


def outputs(con: duckdb.DuckDBPyConnection, resolutions: list[int]) -> list[str]:
    if not beahiv.available(con):
        return []
    return [
        *geo_lookups.unit_outputs(beahiv.BEAHIV_UNIT),
        *overlap_lookups.unit_outputs(con, beahiv.BEAHIV_UNIT),
        *retail_centre_lookups.unit_outputs(con, beahiv.BEAHIV_UNIT),
    ]


STEP = TransformStep(
    name="beahiv_lookups",
    build=build,
    outputs=outputs,
    description="Per-cell lookups on the BEAHIV grid: its ONS geography codes (max-overlap), every overlapping feature, and its nearest retail centre.",
    depends_on=("beahiv_counts",),
    extract_inputs=(
        *geo_lookups.STEP.extract_inputs,
        *overlap_lookups.STEP.extract_inputs,
        *retail_centre_lookups.STEP.extract_inputs,
    ),
)
