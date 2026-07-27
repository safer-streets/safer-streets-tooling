"""The grids the per-cell lookups and ``{grid}_geogs`` are built over: the H3 resolutions + BEAHIV.

A :class:`Grid` is everything the lookup steps need to know about one gridding of Great Britain — the
table-name infix, the crime-count table whose distinct ``spatial_id`` values *are* its cells, a
subquery turning each ``spatial_id`` into its BNG polygon, and the cell's area. The lookups
(``geo_lookups``, ``overlap_lookups``, ``retail_centre_lookups``) and ``geogs`` iterate grids rather
than H3 resolutions, so one set of steps builds both griddings: they differ only in how a
``spatial_id`` becomes a polygon, and everything downstream of that — the max-overlap geography
assignment, the overlap lists, the nearest retail centre — is grid-agnostic.

H3 cell boundaries come from the h3 extension in WGS-84 and are reprojected; BEAHIV is natively
EPSG:27700, so its cells need no reprojection at all. What BEAHIV needs instead is a way to get from
a ``spatial_id`` to a cell centre, which DuckDB has no function for (and it can't even cast the
16-char hex id to ``UBIGINT`` — ``from_hex`` yields a BLOB and that cast is unimplemented). A
vectorised UDF supplies the centre; the hexagon itself is built in SQL from constant vertex offsets,
since every cell of a given side length and orientation is the same hexagon translated.
"""

import math
from dataclasses import dataclass

import duckdb
import numpy as np
import pyarrow as pa
from beahiv import Orientation, cell_polygon, centroid, encode
from duckdb.sqltypes import DOUBLE, VARCHAR, DuckDBPyType

from safer_streets_tooling.transform.base import register_udf, table_exists

# the BEAHIV grid's parameters. They live here rather than in beahiv_counts because they define the
# *grid*, which the lookups need without importing the counting step (which in turn imports these).
SIDE_LENGTH = 202
ORIENTATION = Orientation.FLAT

_CENTRE_UDF = "beahiv_cell_centre"
_CENTRE_TYPE = DuckDBPyType({"x": DOUBLE, "y": DOUBLE})


def _cell_centre(spatial_id: pa.ChunkedArray) -> pa.StructArray:
    """Decode a vector of BEAHIV ``spatial_id`` hex strings to their EPSG:27700 cell centres.

    The ids are fixed-width 16-char hex, so concatenating a whole DuckDB vector of them and running
    that through ``bytes.fromhex`` produces exactly a big-endian ``uint64`` buffer — the decode is
    then pure numpy, with no per-row Python at all. Mirrors the encoder in :mod:`.beahiv_counts`: an
    ``type="arrow"`` UDF is handed the whole vector as a pyarrow array rather than a row at a time.
    """
    ids = np.frombuffer(bytes.fromhex("".join(spatial_id.to_pylist())), dtype=">u8")
    x, y = centroid(ids)
    return pa.StructArray.from_arrays([pa.array(x), pa.array(y)], names=["x", "y"])


def _vertex_offsets() -> list[tuple[float, float]]:
    """The (dx, dy) metres from a cell's centre to each vertex, closing the ring — from beahiv itself.

    Every cell of a given side length and orientation is the same hexagon translated, so one
    reference cell's polygon minus its own centre gives offsets valid for the entire grid. Deriving
    them from ``cell_polygon`` rather than restating beahiv's vertex angles here keeps the SQL in
    step with beahiv's geometry instead of duplicating it.
    """
    reference = encode(0, 0, SIDE_LENGTH, ORIENTATION)
    cx, cy = centroid(reference)
    ring = cell_polygon(reference)
    return [(x - cx, y - cy) for x, y in [*ring, ring[0]]]


# a flat hexagon of side s has area 3*sqrt(3)/2 * s^2. It is a constant because the grid is
# equal-area in EPSG:27700 — and a *planar* BNG area, which is the right denominator for the
# planar {prefix}_overlap_area columns, unlike h3_cell_area's geodesic m^2.
_CELL_AREA = 1.5 * math.sqrt(3.0) * SIDE_LENGTH**2

_HEX_RING = ", ".join(f"ST_Point(c.x + {dx!r}, c.y + {dy!r})" for dx, dy in _vertex_offsets())


@dataclass(frozen=True)
class Grid:
    """One gridding of Great Britain that the per-cell lookups are built over."""

    key: str  # table-name infix: {key}_{...}_lookup, {key}_geogs — "h3_9" / "beahiv_202"
    cells: str  # subquery yielding one row per cell: (spatial_id, cell_geom), cell_geom in BNG
    cell_area: str  # SQL expr for the cell's area in m², over `base.spatial_id` (the alias geogs uses)

    @property
    def counts_table(self) -> str:
        """The crime-count table whose distinct ``spatial_id`` values are this grid's cells.

        One rule for every grid: ``h3_9`` → ``crime_counts_h3_9``, ``beahiv_202`` →
        ``crime_counts_beahiv_202``.
        """
        return f"crime_counts_{self.key}"


def h3_grid(res: int) -> Grid:
    """The H3 grid at resolution ``res``, its cells reprojected from the h3 extension's WGS-84 boundary."""
    key = f"h3_{res}"
    return Grid(
        key=key,
        cells=f"""(
            SELECT DISTINCT
                spatial_id,
                ST_Transform(
                    ST_GeomFromText(h3_cell_to_boundary_wkt(spatial_id)),
                    'EPSG:4326', 'EPSG:27700', always_xy := true
                ) AS cell_geom
            FROM crime_counts_{key}
        )""",
        cell_area="h3_cell_area(base.spatial_id, 'm^2')",
    )


BEAHIV_GRID = Grid(
    key=f"beahiv_{SIDE_LENGTH}",
    # the UDF is called once per cell, in the inner query, rather than once per vertex it feeds
    cells=f"""(
        SELECT spatial_id, ST_MakePolygon(ST_MakeLine([{_HEX_RING}])) AS cell_geom
        FROM (SELECT DISTINCT spatial_id, {_CENTRE_UDF}(spatial_id) AS c FROM crime_counts_beahiv_{SIDE_LENGTH})
    )""",
    # cast explicitly: DuckDB reads a bare decimal literal as DECIMAL, and cell_area must be the
    # DOUBLE the H3 grids' h3_cell_area returns so every *_geogs table has one schema
    cell_area=f"{_CELL_AREA!r}::DOUBLE",
)


def grids(con: duckdb.DuckDBPyConnection, resolutions: list[int]) -> list[Grid]:
    """Every grid the lookups build over: one per H3 resolution, plus BEAHIV when its counts exist.

    Registering the BEAHIV centre UDF is folded in here rather than left to each caller: the grid's
    ``cells`` SQL is unusable without it, so the two belong together and no step can forget it.
    Registration is idempotent and lock-guarded, so the concurrent lookup steps can each call this.

    The BEAHIV grid is skipped when ``crime_counts_beahiv_*`` is absent — mirroring how an overlap
    feature is skipped when its source table wasn't loaded — which keeps ``outputs`` honest about
    what ``build`` will actually create.
    """
    out = [h3_grid(res) for res in resolutions]
    if table_exists(con, BEAHIV_GRID.counts_table):
        register_udf(con, _CENTRE_UDF, _cell_centre, [VARCHAR], _CENTRE_TYPE)
        out.append(BEAHIV_GRID)
    return out
