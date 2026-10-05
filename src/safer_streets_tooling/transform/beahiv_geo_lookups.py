"""``beahiv202_{key}_lookup`` — each BEAHIV cell mapped to one ONS geography code (in memory only).

The BEAHIV counterpart of :mod:`.geo_lookups`: the same :func:`~.geo_lookups.build_unit` over
:data:`~safer_streets_tooling.transform.beahiv.BEAHIV_UNIT`. Like the H3 step it declares no ``outputs``, so
it is never served from the parquet cache and always builds: the lookups are only ever in memory, and
``beahiv_geogs`` needs them whenever it rebuilds. Folded into ``beahiv_lookups`` (which *is* cached), a
cached run skipped them and a ``beahiv_geogs`` rebuild then failed on a missing table.
"""

import duckdb

from safer_streets_tooling.transform import beahiv, geo_lookups
from safer_streets_tooling.transform.base import Grid, TransformStep


def build(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Build every ``beahiv202_{key}_lookup``."""
    if not beahiv.available(con):
        return
    beahiv.register_udfs(con)  # BEAHIV_UNIT.cells calls the cell-polygon UDF
    geo_lookups.build_unit(con, beahiv.BEAHIV_UNIT, replace)


def outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    return []


STEP = TransformStep(
    name="beahiv_geo_lookups",
    build=build,
    outputs=outputs,
    grid=Grid.BEAHIV,
    description="Per-cell ONS geography codes (max-overlap) on the BEAHIV grid; in memory only, folded into beahiv202_geogs.",
    depends_on=("beahiv_counts",),
    extract_inputs=geo_lookups.STEP.extract_inputs,
)
