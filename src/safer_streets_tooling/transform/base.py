"""Step primitives shared by the transform pipeline.

A :class:`TransformStep` describes one aggregation step: how to *build* its relations against a
DuckDB connection, which relation names it *outputs* (so the pipeline can cache/skip it), and which
other steps it ``depends_on``. The registry in ``safer_streets_tooling.transform`` lists the steps in
dependency order and the pipeline wires them into an ``AsyncPipeline`` — mirroring how the extract
phase turns ``Dataset`` entries into nodes.

A :class:`SpatialUnit` describes the *grid* a step aggregates onto — an H3 resolution, or the Home
Office hotspot hexes (``safer_streets_tooling.transform.hotspots``). Both are keyed by ``spatial_id``
and their relation names differ only by the unit's ``key``, so the per-cell steps build the same SQL
for either.

The transforms operate on an open, writable DuckDB connection that already contains a ``crime_data``
table (street-level crimes) and one boundary table per ONS geography (each with a ``spatial_id`` code
and a BNG ``geom`` column). Ported from the ``duckdb-spatial`` prototype notebook (safer-streets-eda).
"""

from collections.abc import Callable
from dataclasses import dataclass, field

import duckdb

H3_RESOLUTIONS = [9]


@dataclass(frozen=True)
class SpatialUnit:
    """One grid the per-cell transforms aggregate onto (an H3 resolution, or the hotspot hexes).

    ``key`` is the infix every relation name carries — ``h3_9`` gives ``crime_counts_h3_9`` /
    ``h3_9_geogs``, ``hotspots`` gives ``crime_counts_hotspots`` / ``hotspots_geogs``.

    ``cells`` is a subquery yielding one row per unit: its ``spatial_id`` and ``cell_geom``, the unit's
    boundary in BNG (the CRS every geometry the lookups intersect it with is in). ``area`` is the unit's
    area in m² as an expression usable in ``geogs``, where the base relation is aliased ``base``;
    ``area_join`` is any extra join that expression needs.
    """

    key: str
    cells: str
    area: str
    area_join: str = ""


def h3_unit(res: int) -> SpatialUnit:
    """The H3 grid at resolution ``res``, whose cells are those carrying crimes.

    The cells are taken from ``crime_counts_h3_{res}`` (so the grid is exactly the crime grid),
    de-duplicated as ids before their boundary is materialised — much cheaper than a DISTINCT over the
    polygons. ``h3_cell_area`` gives the cell's true (geodesic) area straight from the id.
    """
    return SpatialUnit(
        key=f"h3_{res}",
        cells=f"""
            SELECT
                spatial_id,
                ST_Transform(
                    ST_GeomFromText(h3_cell_to_boundary_wkt(spatial_id)),
                    'EPSG:4326', 'EPSG:27700', always_xy := true
                ) AS cell_geom
            FROM (SELECT DISTINCT spatial_id FROM crime_counts_h3_{res})
        """,
        area="h3_cell_area(base.spatial_id, 'm^2')",
    )


@dataclass(frozen=True)
class TransformStep:
    """One H3 aggregation step in the transform pipeline.

    ``build(con, resolutions, replace)`` creates the step's relations; ``outputs(con, resolutions)``
    returns the relation names it produces (used to cache them as parquet and skip rebuilds).
    ``description`` is a one-line human summary of the relation(s) the step produces, surfaced in the
    ``index.parquet`` catalogue (keep it current when the outputs change). ``depends_on`` lists the names
    of steps whose relations this one reads. ``extract_inputs`` lists the extract dataset names this step
    reads (their parquet live in the extract dir); together with the output parquet of its ``depends_on``
    steps they are the step's inputs for staleness checks — the cached output is reused only when it
    exists *and* is newer than every input.
    """

    name: str
    build: Callable[[duckdb.DuckDBPyConnection, list[int], bool], None]
    outputs: Callable[[duckdb.DuckDBPyConnection, list[int]], list[str]]
    description: str = ""
    depends_on: tuple[str, ...] = field(default_factory=tuple)
    extract_inputs: tuple[str, ...] = field(default_factory=tuple)


def create_clause(kind: str, name: str, *, replace: bool) -> str:
    """Build the leading CREATE clause for a table or view.

    replace=True  -> ``CREATE OR REPLACE {kind} {name}``    (always rebuilt)
    replace=False -> ``CREATE {kind} IF NOT EXISTS {name}`` (kept if it already exists)
    """
    return f"CREATE OR REPLACE {kind} {name}" if replace else f"CREATE {kind} IF NOT EXISTS {name}"


def table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    return (
        con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ? AND table_schema = 'main'",
            [name],
        ).fetchone()[0]  # ty:ignore[not-subscriptable]
        > 0
    )
