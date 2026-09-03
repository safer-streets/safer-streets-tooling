"""Step registry for the transform pipeline.

``STEPS`` is the ordered catalogue the transform orchestrator runs. Each entry builds one set of
aggregation relations (counts, the per-cell lookups, or the ``*_geogs`` attributes) and declares the
relation names it produces (so they can be cached as parquet). Steps are ordered so that every
``depends_on`` precedes its dependent (validated at import time), and the pipeline wires them into an
``AsyncPipeline`` — mirroring how ``safer_streets_tooling.extract`` turns ``Dataset`` entries into nodes.

The steps come in two families over the two spatial units: the H3 grid at each requested resolution,
and the Home Office hotspot hexes (the ``hotspot_*`` steps, a no-op when that optional extract is
absent). Both produce the same relations keyed by ``spatial_id``, differing only in the unit's name.
"""

from safer_streets_tooling.transform import (
    building_counts,
    crime_counts,
    geo_lookups,
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
from safer_streets_tooling.transform.base import TransformStep

STEPS: tuple[TransformStep, ...] = (
    crime_counts.STEP,
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
)


def _validate(steps: tuple[TransformStep, ...]) -> None:
    """Names are unique, every depends_on refers to an earlier step, and each has a description.

    The description is required so the ``index.parquet`` catalogue never has a blank row (see AGENTS.md).
    """
    seen: set[str] = set()
    for step in steps:
        if step.name in seen:
            raise ValueError(f"duplicate transform step name: {step.name}")
        if not step.description.strip():
            raise ValueError(f"transform step {step.name!r} needs a non-empty description (surfaced in index.parquet)")
        for dep in step.depends_on:
            if dep not in seen:
                raise ValueError(f"transform step {step.name!r} depends on {dep!r}, which is not registered earlier")
        seen.add(step.name)


_validate(STEPS)

BY_NAME: dict[str, TransformStep] = {step.name: step for step in STEPS}

from safer_streets_tooling.transform.pipeline import (  # noqa: E402
    TransformNode,
    build_all,
    build_pipeline,
)

__all__ = [
    "BY_NAME",
    "STEPS",
    "TransformNode",
    "TransformStep",
    "build_all",
    "build_pipeline",
]
