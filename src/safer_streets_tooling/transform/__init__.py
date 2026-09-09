"""Step registry for the transform pipeline.

``STEPS`` is the ordered catalogue the transform orchestrator runs. Each entry builds one set of
aggregation relations (counts, the per-cell lookups, or the ``*_geogs`` attributes) and declares the
relation names it produces (so they can be cached as parquet). Steps are ordered so that every
``depends_on`` precedes its dependent (validated at import time), and the pipeline wires them into an
``AsyncPipeline`` — mirroring how ``safer_streets_tooling.extract`` turns ``Dataset`` entries into nodes.

The steps come in three families over the three spatial units, each declaring its
:class:`~safer_streets_tooling.transform.base.Grid`: the H3 grid at each resolution in
``H3_RESOLUTIONS`` (``Grid.H3``), the Home Office hotspot hexes (the ``hotspot_*`` steps, ``Grid.HO``, a
no-op when that optional extract is absent), and the BEAHIV 202m hex grid (the ``beahiv_*`` steps,
``Grid.BEAHIV``). All produce the same relations keyed by ``spatial_id``, differing in the unit's name —
and, for BEAHIV, in that column's type: an integer cell id rather than the hex string the other two
units use. No step depends on one in another family (checked at import), so a run can be narrowed to
any subset of the grids: ``data transform --grid beahiv``.
"""

from safer_streets_tooling.transform import (
    beahiv_counts,
    beahiv_geogs,
    beahiv_lookups,
    building_counts,
    crime_counts,
    geo_lookups,
    geography_counts,
    geogs,
    hotspot_counts,
    hotspot_geogs,
    hotspot_lookups,
    overlap_lookups,
    population_counts,
    retail_centre_lookups,
    road_intersection_counts,
    streetlight_counts,
)
from safer_streets_tooling.transform.base import Grid, TransformStep

STEPS: tuple[TransformStep, ...] = (
    crime_counts.STEP,
    geography_counts.STEP,  # independent: the ONS geography counts, their own family (one OA join)
    streetlight_counts.STEP,  # independent: counts the streetlights extract per res-9 cell
    building_counts.STEP,  # depends on crime_counts: buildings per res-9 cell, restricted to its cells
    population_counts.STEP,  # independent: OA residential + workplace population per res-9 cell via buildings
    road_intersection_counts.STEP,  # depends on crime_counts: intersections per cell, restricted to its cells
    geo_lookups.STEP,  # depends on crime_counts
    overlap_lookups.STEP,  # depends on crime_counts
    retail_centre_lookups.STEP,  # depends on crime_counts
    geogs.STEP,  # depends on the three lookups
    hotspot_counts.STEP,  # independent: the same counts on the hotspot hexes (their own grid)
    hotspot_lookups.STEP,  # independent: the three lookups on the hotspot hexes
    hotspot_geogs.STEP,  # depends on hotspot_lookups
    beahiv_counts.STEP,  # independent: the same crime counts on the BEAHIV hex grid
    beahiv_lookups.STEP,  # depends on beahiv_counts: the three lookups on the BEAHIV cells
    beahiv_geogs.STEP,  # depends on beahiv_lookups
)


def _validate(steps: tuple[TransformStep, ...]) -> None:
    """Names are unique, every depends_on refers to an earlier step *in the same grid family*, and each
    has a description.

    The description is required so the ``index.parquet`` catalogue never has a blank row (see AGENTS.md).
    The same-family rule is what makes ``--grid`` safe: a build narrowed to one family drops every other
    step, so a cross-family dependency would silently run against relations that were never built.
    """
    seen: dict[str, TransformStep] = {}
    for step in steps:
        if step.name in seen:
            raise ValueError(f"duplicate transform step name: {step.name}")
        if not step.description.strip():
            raise ValueError(f"transform step {step.name!r} needs a non-empty description (surfaced in index.parquet)")
        for dep in step.depends_on:
            if dep not in seen:
                raise ValueError(f"transform step {step.name!r} depends on {dep!r}, which is not registered earlier")
            if seen[dep].grid is not step.grid:
                raise ValueError(
                    f"transform step {step.name!r} ({step.grid}) depends on {dep!r} ({seen[dep].grid}), which is on "
                    f"another grid — a --grid subset would drop it"
                )
        seen[step.name] = step


_validate(STEPS)

BY_NAME: dict[str, TransformStep] = {step.name: step for step in STEPS}

from safer_streets_tooling.transform.pipeline import (  # noqa: E402
    ALL_GRIDS,
    TransformNode,
    build_all,
    build_pipeline,
)

__all__ = [
    "ALL_GRIDS",
    "BY_NAME",
    "STEPS",
    "Grid",
    "TransformNode",
    "TransformStep",
    "build_all",
    "build_pipeline",
]
