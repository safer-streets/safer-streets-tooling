"""Dataset registry primitives for the modular build pipeline.

A :class:`Dataset` describes one source: how to *extract* it (download + preprocess in its own
in-memory DuckDB, then dump to a GeoParquet file) and how it lands in the final database (table
name, whether it carries geometry, what upstream parquet it needs). The orchestrator in
``scripts.build_db`` iterates the registry rather than hard-coding per-dataset control flow, so a
new source is added by writing one module and appending its ``Dataset`` to ``DATASETS``.

All geometry is British National Grid (EPSG:27700) by convention; see ``safer_streets_tooling.datasets._common``.
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class ExtractContext:
    """Inputs handed to a dataset's ``extract`` function.

    ``staging`` is the directory holding the ``<name>.parquet`` intermediates; an extractor reads
    any upstream dataset it depends on via ``parquet(dep_name)`` and writes its own output there.
    """

    staging: Path
    force_download: bool = False

    def parquet(self, name: str) -> Path:
        return self.staging / f"{name}.parquet"


@dataclass(frozen=True)
class Dataset:
    """One source in the build pipeline.

    ``extract`` downloads/preprocesses the source in its own connection and writes
    ``ctx.parquet(name)``; it raises (e.g. ``FileNotFoundError``) when an absent source means the
    dataset should be skipped. ``optional`` datasets are skipped with a warning on failure, required
    ones abort the build. ``geometry`` flags that the table carries a ``geom`` column (so it is
    RTree-indexed on assemble). ``depends_on`` lists other dataset names whose parquet this extractor
    reads, and must precede this one in ``DATASETS``.
    """

    name: str
    table: str
    extract: Callable[[ExtractContext], None]
    optional: bool = True
    geometry: bool = True
    depends_on: tuple[str, ...] = field(default_factory=tuple)
