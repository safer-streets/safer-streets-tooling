"""The BEAHIV 202m hex grid as a spatial unit: the two UDFs it needs, and :data:`BEAHIV_UNIT`.

The transform's third spatial unit, alongside the H3 cells and the Home Office hotspot hexes. Every
relation built on it is named with the ``beahiv202`` key (``beahiv202_crime_counts``,
``beahiv202_geogs``, …) and keyed by ``spatial_id``, so a consumer joins the BEAHIV counts and
attributes exactly as it joins the H3 ones — which is the point of the grid: the same crimes on an
equal-area hexagonal gridding, comparable cell for cell with H3 resolution 9.

Like the H3 units and unlike the hotspot hexes, the cells are those *carrying crimes* — taken from
``beahiv202_crime_counts`` rather than from the ``beahiv202`` extract, which tiles the whole of
England & Wales (~1.5m cells) and would put the great majority of them through the lookups for
nothing. That also keeps the grid exactly the crime grid, as it is for H3.

Two things differ from H3 and are captured here, once, as ``type="arrow"`` UDFs over beahiv — DuckDB
has no BEAHIV function for either:

* a crime point becomes a cell id by *arithmetic* rather than by a spatial join or an h3 extension
  function, so :func:`register_udfs` supplies ``beahiv_cell_from_bng`` for the counts step;
* a ``spatial_id`` becomes a polygon by decoding it, so the same call supplies
  ``beahiv_cell_polygon``, which hands a whole vector of ids to beahiv's ``cell_polygons`` and returns
  WKB for ``ST_GeomFromWKB``. The geometry is beahiv's own, not reconstructed here.

``spatial_id`` is the cell id as a plain ``BIGINT``. beahiv reserves its top three bits, which puts
every id below 2**61, so the natural integer column is signed and no unsigned type or hex-string
encoding is needed to hold one.
"""

import duckdb
import numpy as np
import pyarrow as pa
import shapely
from beahiv import cell_polygons
from duckdb.sqltypes import BIGINT, BLOB

from safer_streets_tooling.beahiv_grid import CELL_AREA, ENCODE_UDF, KEY, register_encoder
from safer_streets_tooling.grids import BEAHIV_ID
from safer_streets_tooling.transform.base import SpatialUnit, column_exists, register_udf, relation, table_exists
from safer_streets_tooling.transform.crime_counts import DATASET as CRIME_COUNTS

# re-exported: the encoder moved to `beahiv_grid` (the extract phase tags features with a cell too),
# but the counts step reaches for it here, alongside this grid's other UDF
__all__ = ["BEAHIV_UNIT", "COUNTS_TABLE", "ENCODE_UDF", "available", "register_udfs", "tagged"]

COUNTS_TABLE = relation(KEY, CRIME_COUNTS)

_POLYGON_UDF = "beahiv_cell_polygon"


def _cell_polygon_wkb(spatial_id: pa.ChunkedArray) -> pa.Array:
    """Decode a vector of BEAHIV ``spatial_id`` values to their cell outlines, as WKB in EPSG:27700.

    beahiv's ``cell_polygons`` takes the whole array of ids at once (the signed integers DuckDB hands
    over need no conversion — it coerces numpy integers itself) and ``shapely.to_wkb`` serialises the
    whole array at once too, so a DuckDB vector crosses into Python and back with no per-row work.

    WKB rather than WKT: it is the shorter, exact binary form, so nothing is lost to decimal rounding
    on the way through. The polygons are beahiv's own — this module does not build a hexagon.
    """
    return pa.array(shapely.to_wkb(np.asarray(cell_polygons(np.asarray(spatial_id)), dtype=object)))


def register_udfs(con: duckdb.DuckDBPyConnection) -> None:
    """Register both BEAHIV UDFs on ``con`` (idempotent — see :func:`.base.register_udf`).

    Every step touching this grid calls it, including the ones that only read the lookups: those are
    *views* carrying the ``beahiv_cell_polygon`` call, so the UDF has to be in the catalog whenever one
    is evaluated, not merely when it is created.
    """
    register_encoder(con)  # the BNG -> cell encoder, shared with the extract phase
    register_udf(con, _POLYGON_UDF, _cell_polygon_wkb, [BIGINT], BLOB)


# The BEAHIV grid. `cells` needs the polygon UDF registered (see register_udfs); `area` is a constant
# because the grid is equal-area, so it needs no area_join.
BEAHIV_UNIT = SpatialUnit(
    key=KEY,
    cells=f"""
        SELECT spatial_id, ST_GeomFromWKB({_POLYGON_UDF}(spatial_id)) AS cell_geom
        FROM (SELECT DISTINCT spatial_id FROM {COUNTS_TABLE})
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


def tagged(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    """True when ``table`` carries this grid's cell id, so it can be counted onto the grid.

    The extract tags every point layer with its cell (:data:`~safer_streets_tooling.grids.BEAHIV_ID`),
    so counting onto this grid is a group-and-count rather than a spatial join — but a parquet extracted
    before that column existed has to be skipped until it is re-extracted, which is what the column
    check is for.

    Deliberately *not* gated on :func:`available`. A step's ``outputs`` is resolved before its ``build``
    runs, so a check for a relation the same step creates reads False at exactly the moment the pipeline
    is deciding which parquet to write — the counts would be built in memory and then silently not
    persisted. Nothing here needs the crime counts anyway: a cell comes from the feature's own
    coordinates.
    """
    return table_exists(con, table) and column_exists(con, table, BEAHIV_ID)
