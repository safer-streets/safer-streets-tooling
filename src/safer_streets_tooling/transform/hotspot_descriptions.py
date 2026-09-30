"""``hotspots_descriptions`` — a human-readable location for every Home Office hotspot hex.

The hotspot counterpart of ``beahiv202_descriptions``: the same labels, built by the same query
(:func:`safer_streets_tooling.transform.beahiv_descriptions.build_unit`) over ``hotspots_geogs`` and the
hotspot lookups. One clause differs: the school. Its site is found from the cell id each school is tagged
with at extract, and there is no hotspot-hex tag, so the clause drops out — the ``school_ids`` in
``hotspots_geogs`` are walking catchments, which would make "near" mean something else on this grid.

Like the rest of the hotspot family the table is local-only (see :mod:`safer_streets_tooling.local_only`):
its ``hotspots_`` name keeps it out of ``data sync``.
"""

import duckdb

from safer_streets_tooling.transform import beahiv_descriptions, hotspots
from safer_streets_tooling.transform.base import Grid, TransformStep, relation
from safer_streets_tooling.transform.ons_hierarchy import GEOGRAPHY_MAPPINGS


def build(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Build ``hotspots_descriptions``."""
    if not hotspots.available(con):
        return
    beahiv_descriptions.build_unit(con, hotspots.HOTSPOT_UNIT, replace)


def outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [relation(hotspots.HOTSPOT_UNIT.key, beahiv_descriptions.DATASET)] if hotspots.available(con) else []


STEP = TransformStep(
    name="hotspot_descriptions",
    build=build,
    outputs=outputs,
    grid=Grid.HO,
    description="One row per hotspot hex: a human-readable short_location and description, plus the named roads, greenspace and retail centre they are built from.",
    depends_on=("hotspot_lookups", "hotspot_geogs"),
    # no schools: nothing places a school site in a hotspot hex (see the module docstring)
    extract_inputs=(
        "open_roads",
        "open_greenspace",
        "retail_centres",
        *(GEOGRAPHY_MAPPINGS[k] for k in ("lad24cd", "msoa21cd", "lsoa21cd")),
    ),
)
