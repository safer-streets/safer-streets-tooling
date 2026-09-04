"""``*_counts_hotspots`` — the per-cell counts, aggregated onto the Home Office hotspot hexes.

The H3 counts and these are the same measures on a different grid, so each one lives with its H3
counterpart (``crime_counts.build_hotspots`` and friends) and this module only wires them into a single
step. Everything is keyed by ``spatial_id`` — the hex id — so a consumer joins them to
``hotspots_geogs`` exactly as it joins the res-9 counts to ``h3_9_geogs``.

Every relation is skipped when its source extract (or the hotspots extract itself) is absent, so this
step is a clean no-op on a build without the hotspot hexes.
"""

import duckdb

from safer_streets_tooling.transform import (
    building_counts,
    crime_counts,
    population_counts,
    road_intersection_counts,
    streetlight_counts,
)
from safer_streets_tooling.transform.base import TransformStep

# the modules whose counts have a hotspot-hex equivalent; each exposes build_hotspots / hotspot_outputs
_MODULES = (crime_counts, streetlight_counts, building_counts, population_counts, road_intersection_counts)


def build(con: duckdb.DuckDBPyConnection, resolutions: list[int], replace: bool) -> None:
    """Build every hotspot count. ``resolutions`` is ignored — the hexes are their own grid."""
    for module in _MODULES:
        module.build_hotspots(con, replace)


def outputs(con: duckdb.DuckDBPyConnection, resolutions: list[int]) -> list[str]:
    return [name for module in _MODULES for name in module.hotspot_outputs(con)]


STEP = TransformStep(
    name="hotspot_counts",
    build=build,
    outputs=outputs,
    description="Crime / street light / building / population / road-intersection counts per Home Office hotspot hex, keyed by spatial_id.",
    extract_inputs=(
        "hotspots",
        "crime_data",
        "streetlights",
        "buildings",
        "workplace_population",
        "residential_population",
        "road_intersections",
    ),
)
