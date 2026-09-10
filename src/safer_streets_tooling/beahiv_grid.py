"""The BEAHIV grid's parameters, shared by the extract that tiles it and the transform that counts onto it.

Both phases must agree on the side length and orientation or nothing lines up: the extract's
``beahiv202`` cells and the transform's ``beahiv202_crime_counts`` cells are joined on ``spatial_id``,
which *is* the encoded (q, r, side_length, orientation). Two independent copies of these constants
would produce two disjoint grids that still join without error — every row simply missing — so they
live here once and neither phase declares its own.

A 202 m side gives a cell of ~0.106 km², within a percent of an H3 resolution-9 cell, so the two
griddings are directly comparable on identical data — which is the reason this grid exists alongside
H3 rather than replacing it.
"""

import duckdb
import pyarrow as pa
from beahiv import Orientation, bng_to_cell, cell_polygon, encode
from duckdb.sqltypes import BIGINT, DOUBLE

from safer_streets_tooling.duckdb_udf import register_udf

SIDE_LENGTH = 202
ORIENTATION = Orientation.FLAT

# the prefix every relation on this grid carries: the beahiv202 extract, beahiv202_crime_counts,
# beahiv202_geogs, and the beahiv202_id column on the point layers. Derived from the side length so a
# change to the grid renames its tables rather than silently redefining what an existing name means.
# One token, no internal underscore (see `grids`), so a name splits cleanly into grid and dataset.
KEY = f"beahiv{SIDE_LENGTH}"

# A constant, because the grid is equal-area in EPSG:27700 — and a *planar* BNG area, which is the
# right denominator for the planar {prefix}_overlap_area columns, unlike h3_cell_area's geodesic m².
# Measured off beahiv's own reference cell rather than restating `3*sqrt(3)/2 * s²` here, for the same
# reason the transform takes its cell polygons straight from beahiv (see transform.beahiv): beahiv
# owns the cell's geometry, so anything derived from it should come from beahiv and not be
# reimplemented against it. Exact to the last bit either way, and computed once at import.
CELL_AREA = cell_polygon(encode(0, 0, SIDE_LENGTH, ORIENTATION)).area

# DuckDB has no BEAHIV function, so encoding a point is a vectorised (``type="arrow"``) Python UDF.
# It lives here rather than with the transform because both phases need it: the extract tags each
# feature with its cell, and the transform counts crimes onto the same grid.
ENCODE_UDF = "beahiv_cell_from_bng"


def _cell_from_bng(x: pa.ChunkedArray, y: pa.ChunkedArray) -> pa.Array:
    """Encode a vector of BNG (x, y) metres as BEAHIV cell ids.

    A pyarrow array in gives a pyarrow array back, so the DuckDB vectors pass straight through to
    beahiv and back; the cast to ``int64`` is the only work here, matching the BIGINT the UDF
    declares (beahiv returns the ids as ``uint64``, and every one of them fits — beahiv reserves its
    top three bits, which puts every id below 2**61).

    It takes projected coordinates rather than lat/lon, for two reasons in order of importance:

    1. ``latlon_to_cell`` reprojects with pyproj, and calling pyproj from DuckDB's worker threads
       **segfaults the process** (reproducible on the full crime extract; survives only at
       ``threads = 1``, and neither a lock nor a thread-local ``Transformer`` avoids it). The
       transform phase runs with ``threads = 4``, so that path is unusable here.
    2. every layer that needs a cell id already holds a BNG geometry — projected once in the
       extractor — so going via lat/lon would reproject coordinates we already have, at roughly
       double the cost.

    Verified equivalent: identical cell ids to ``latlon_to_cell`` on 2M rows of the extract.
    """
    return bng_to_cell(x, y, SIDE_LENGTH, ORIENTATION).cast(pa.int64())


def register_encoder(con: duckdb.DuckDBPyConnection) -> None:
    """Register :data:`ENCODE_UDF` on ``con`` (idempotent — see :func:`.duckdb_udf.register_udf`)."""
    register_udf(con, ENCODE_UDF, _cell_from_bng, [DOUBLE, DOUBLE], BIGINT)


def cell_id_sql(bng_geom: str) -> str:
    """SQL for a feature's BEAHIV cell id, given an expression for its BNG point.

    Needs :func:`register_encoder` called on the connection first.
    """
    return f"{ENCODE_UDF}(ST_X({bng_geom}), ST_Y({bng_geom}))"
