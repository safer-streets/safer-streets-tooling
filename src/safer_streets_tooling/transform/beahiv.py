"""The BEAHIV 202m hex grid as a spatial unit: the two UDFs it needs, and :data:`BEAHIV_UNIT`.

The transform's third spatial unit, alongside the H3 cells and the Home Office hotspot hexes. Every
relation built on it is named with the ``beahiv_202`` key (``crime_counts_beahiv_202``,
``beahiv_202_geogs``, …) and keyed by ``spatial_id``, so a consumer joins the BEAHIV counts and
attributes exactly as it joins the H3 ones — which is the point of the grid: the same crimes on an
equal-area hexagonal gridding, comparable cell for cell with H3 resolution 9.

Like the H3 units and unlike the hotspot hexes, the cells are those *carrying crimes* — taken from
``crime_counts_beahiv_202`` rather than from the ``beahiv_202`` extract, which tiles the whole of
England & Wales (~1.5m cells) and would put the great majority of them through the lookups for
nothing. That also keeps the grid exactly the crime grid, as it is for H3.

Two things differ from H3 and are captured here, once, as ``type="arrow"`` UDFs over beahiv:

* a crime point becomes a cell id by *arithmetic* rather than by a spatial join or an h3 extension
  function, so :func:`register_udfs` supplies ``beahiv_cell_from_bng`` for the counts step;
* a ``spatial_id`` becomes a polygon by decoding it, which DuckDB has no function for, so the same
  call supplies ``beahiv_cell_centre`` and :data:`BEAHIV_UNIT` builds the hexagon in SQL from that
  centre plus constant vertex offsets — every cell of a given side length and orientation is the same
  hexagon translated, so the offsets are computed once from beahiv itself.

``spatial_id`` is the cell id as a plain ``BIGINT``. beahiv reserves its top three bits, which puts
every id below 2**61, so the natural integer column is signed and no unsigned type or hex-string
encoding is needed to hold one.
"""

import duckdb
import numpy as np
import pyarrow as pa
from beahiv import bng_to_cell, cell_polygon, centroid, encode
from duckdb.sqltypes import BIGINT, DOUBLE, DuckDBPyType

from safer_streets_tooling.beahiv_grid import CELL_AREA, KEY, ORIENTATION, SIDE_LENGTH
from safer_streets_tooling.transform.base import SpatialUnit, register_udf, table_exists

COUNTS_TABLE = f"crime_counts_{KEY}"

ENCODE_UDF = "beahiv_cell_from_bng"
_CENTRE_UDF = "beahiv_cell_centre"
_CENTRE_TYPE = DuckDBPyType({"x": DOUBLE, "y": DOUBLE})


def _cell_from_bng(x: pa.ChunkedArray, y: pa.ChunkedArray) -> pa.Array:
    """Encode a vector of BNG (x, y) metres as BEAHIV cell ids.

    A pyarrow array in gives a pyarrow array back, so the DuckDB vectors pass straight through to
    beahiv and back; the cast to ``int64`` is the only work here, matching the BIGINT the UDF
    declares (beahiv returns the ids as ``uint64``, and every one of them fits — see the module
    docstring).

    It takes projected coordinates rather than lat/lon, for two reasons in order of importance:

    1. ``latlon_to_cell`` reprojects with pyproj, and calling pyproj from DuckDB's worker threads
       **segfaults the process** (reproducible on the full crime extract; survives only at
       ``threads = 1``, and neither a lock nor a thread-local ``Transformer`` avoids it). The
       transform phase runs with ``threads = 4``, so that path is unusable here.
    2. ``crime_data.geom`` is already BNG — projected once in the extractor — so going via lat/lon
       would reproject coordinates we already hold, at roughly double the cost.

    Verified equivalent: identical cell ids to ``latlon_to_cell`` on 2M rows of the extract.
    """
    return bng_to_cell(x, y, SIDE_LENGTH, ORIENTATION).cast(pa.int64())


def _cell_centre(spatial_id: pa.ChunkedArray) -> pa.StructArray:
    """Decode a vector of BEAHIV ``spatial_id`` values to their EPSG:27700 cell centres.

    Pure numpy, with no per-row Python at all: beahiv's ``centroid`` takes the whole array of ids and
    the signed integers DuckDB hands over need no conversion, since it coerces numpy integers itself.
    """
    x, y = centroid(np.asarray(spatial_id))
    return pa.StructArray.from_arrays([pa.array(x), pa.array(y)], names=["x", "y"])


def register_udfs(con: duckdb.DuckDBPyConnection) -> None:
    """Register both BEAHIV UDFs on ``con`` (idempotent — see :func:`.base.register_udf`).

    Every step touching this grid calls it, including the ones that only read the lookups: those are
    *views* carrying the ``beahiv_cell_centre`` call, so the UDF has to be in the catalog whenever one
    is evaluated, not merely when it is created.
    """
    register_udf(con, ENCODE_UDF, _cell_from_bng, [DOUBLE, DOUBLE], BIGINT)
    register_udf(con, _CENTRE_UDF, _cell_centre, [BIGINT], _CENTRE_TYPE)


def _vertex_offsets() -> list[tuple[float, float]]:
    """The (dx, dy) metres from a cell's centre to each vertex, closing the ring — from beahiv itself.

    Every cell of a given side length and orientation is the same hexagon translated, so one
    reference cell's polygon minus its own centre gives offsets valid for the entire grid. Deriving
    them from ``cell_polygon`` rather than restating beahiv's vertex angles here keeps the SQL in
    step with beahiv's geometry instead of duplicating it. Shapely's exterior ring already repeats
    the first vertex, which is the closing point ``ST_MakePolygon`` needs.
    """
    reference = encode(0, 0, SIDE_LENGTH, ORIENTATION)
    cx, cy = centroid(reference)
    return [(x - cx, y - cy) for x, y in cell_polygon(reference).exterior.coords]


_HEX_RING = ", ".join(f"ST_Point(ctr.x + {dx!r}, ctr.y + {dy!r})" for dx, dy in _vertex_offsets())

# The BEAHIV grid. `cells` needs the centre UDF registered (see register_udfs); `area` is a constant
# because the grid is equal-area, so it needs no area_join.
BEAHIV_UNIT = SpatialUnit(
    key=KEY,
    # the UDF is called once per cell, in the inner query, rather than once per vertex it feeds
    cells=f"""
        SELECT spatial_id, ST_MakePolygon(ST_MakeLine([{_HEX_RING}])) AS cell_geom
        FROM (SELECT DISTINCT spatial_id, {_CENTRE_UDF}(spatial_id) AS ctr FROM {COUNTS_TABLE})
    """,
    # cast explicitly: DuckDB reads a bare decimal literal as DECIMAL, and cell_area must be the
    # DOUBLE the H3 units' h3_cell_area returns so every *_geogs table has one schema
    area=f"{CELL_AREA!r}::DOUBLE",
)


def available(con: duckdb.DuckDBPyConnection) -> bool:
    """True once the BEAHIV crime counts exist; the lookup and geogs steps are gated on it.

    The counts are this grid's cells, so a build that skipped ``beahiv_counts`` — or ran the lookups
    against a database restored without it — has no grid to look anything up on, and the steps are a
    no-op rather than a failure (mirroring how a hotspot step behaves without its extract).
    """
    return table_exists(con, COUNTS_TABLE)
