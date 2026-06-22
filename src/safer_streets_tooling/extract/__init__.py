"""Dataset registry for the modular build pipeline.

``DATASETS`` is the ordered catalogue the build orchestrator iterates over. Each entry is extracted to
a ``<name>.parquet`` intermediate (its own in-memory DuckDB) and later imported into the final
database. Datasets are ordered so that every ``depends_on`` precedes its dependent (validated at import
time). Add a new source by writing a module that exposes a ``DATASET`` (or ``DATASETS``) and appending
it below.
"""

from safer_streets_tooling.extract import (
    boundaries,
    cctv,
    crime,
    food_outlets,
    greenspace,
    imd,
    land_cover,
    naptan,
    oac,
    poi,
    retail_centres,
    roads,
    schools,
    streetlights,
)
from safer_streets_tooling.extract.base import Dataset, ExtractContext

DATASETS: tuple[Dataset, ...] = (
    crime.DATASET,
    *boundaries.DATASETS,
    greenspace.DATASET,
    land_cover.DATASET,
    retail_centres.DATASET,
    roads.DATASET,
    poi.DATASET,
    naptan.DATASET,
    food_outlets.DATASET,
    streetlights.DATASET,
    cctv.DATASET,
    schools.DATASET,  # depends on open_roads
    imd.DATASET,  # depends on local_authority_districts
    *oac.DATASETS,
)


def _validate(datasets: tuple[Dataset, ...]) -> None:
    """Names are unique and every depends_on refers to an earlier dataset."""
    seen: set[str] = set()
    for ds in datasets:
        if ds.name in seen:
            raise ValueError(f"duplicate dataset name: {ds.name}")
        for dep in ds.depends_on:
            if dep not in seen:
                raise ValueError(f"dataset {ds.name!r} depends on {dep!r}, which is not registered earlier")
        seen.add(ds.name)


_validate(DATASETS)

BY_NAME: dict[str, Dataset] = {ds.name: ds for ds in DATASETS}

from safer_streets_tooling.extract.pipeline import build_pipeline, run_extract  # noqa: E402

__all__ = ["BY_NAME", "DATASETS", "Dataset", "ExtractContext", "build_pipeline", "run_extract"]
