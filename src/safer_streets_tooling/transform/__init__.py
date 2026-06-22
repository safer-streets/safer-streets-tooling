"""Step registry for the H3 transform pipeline.

``STEPS`` is the ordered catalogue the transform orchestrator runs. Each entry builds one set of H3
aggregation relations (counts, the per-cell lookups, or ``h3_{res}_geogs``) and declares the relation
names it produces (so they can be cached as parquet). Steps are ordered so that every ``depends_on``
precedes its dependent (validated at import time), and the pipeline wires them into an ``AsyncPipeline``
— mirroring how ``safer_streets_tooling.extract`` turns ``Dataset`` entries into nodes.
"""

from safer_streets_tooling.transform import (
    crime_counts,
    geo_lookups,
    geogs,
    overlap_lookups,
    retail_centre_lookups,
    streetlight_counts,
)
from safer_streets_tooling.transform.base import TransformStep

STEPS: tuple[TransformStep, ...] = (
    crime_counts.STEP,
    streetlight_counts.STEP,  # independent: counts the streetlights extract per res-9 cell
    geo_lookups.STEP,  # depends on crime_counts
    overlap_lookups.STEP,  # depends on crime_counts
    retail_centre_lookups.STEP,  # depends on crime_counts
    geogs.STEP,  # depends on the three lookups
)


def _validate(steps: tuple[TransformStep, ...]) -> None:
    """Names are unique and every depends_on refers to an earlier step."""
    seen: set[str] = set()
    for step in steps:
        if step.name in seen:
            raise ValueError(f"duplicate transform step name: {step.name}")
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
