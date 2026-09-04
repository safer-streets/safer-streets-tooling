"""The Home Office hotspot hexes as a spatial unit: the ``hotspots`` table and what reads it.

The hotspot grid is the transform's second spatial unit alongside the H3 cells. Every relation built on
it is named with the ``hotspots`` key (``crime_counts_hotspots``, ``hotspots_geogs``,
``building_counts_hotspots``, …) and keyed by ``spatial_id`` — the hex id — so a consumer joins the
hotspot counts and attributes exactly as it joins the H3 ones.

Two things differ from H3 and are captured here, once:

* the cells are *given* polygons rather than derived from a cell id, so :data:`HOTSPOT_UNIT` reads them
  straight from the table and measures their area with ``ST_Area``;
* a point (a crime, a building, a street light) is placed in a hex by a spatial join rather than by
  ``h3_latlng_to_cell``, so :func:`placed_points` writes that join for the count steps.

The whole family is optional: with the ``hotspots`` extract absent (:func:`available` is False) every
hotspot step is a no-op, matching how the other optional layers behave.
"""

import duckdb

from safer_streets_tooling.transform.base import SpatialUnit, table_exists

HOTSPOTS_TABLE = "hotspots"

# The hotspot grid: the supplied polygons are the cells (not just those carrying crimes, unlike the H3
# units), so the hex set is stable and every hotspot relation covers the same 350m hexes.
HOTSPOT_UNIT = SpatialUnit(
    key="hotspots",
    cells=f"SELECT spatial_id, geom AS cell_geom FROM {HOTSPOTS_TABLE}",
    area="ST_Area(hs.geom)",
    area_join=f"LEFT JOIN {HOTSPOTS_TABLE} hs USING (spatial_id)",
)


def available(con: duckdb.DuckDBPyConnection) -> bool:
    """True when the hotspot hexes were extracted; every hotspot build/outputs is gated on it."""
    return table_exists(con, HOTSPOTS_TABLE)


def placed_points(source: str, *cols: str, point: str = "s.geom", where: str = "", outer: bool = False) -> str:
    """A query placing each row of ``source`` (aliased ``s``) in its hotspot hex: ``spatial_id`` + ``cols``.

    ``point`` is the BNG point that locates the row — ``s.geom`` for a point layer, or an expression such
    as ``ST_Centroid(s.geom)`` for a footprint (which is how the extracts derive their ``h3_9_id``, so
    the hotspot counts place a feature the same way the H3 ones do).

    ``ST_Contains`` rather than ``ST_Intersects``: the hexes tile without overlap, so a point exactly on
    a shared edge would otherwise land in two of them — dropping it is the safe failure mode (the same
    choice the ONS geography counts make). Points outside every hex are dropped, which is most of them:
    the hotspot grid covers only the flagged hexes, not all of England & Wales. ``outer=True`` keeps them
    instead, with a NULL ``spatial_id`` — needed when a share of some total is worked out across *all*
    the rows before the ones inside a hex are selected.
    """
    return f"""
        SELECT {", ".join(("h.spatial_id", *cols))}
        FROM {source} s
        {"LEFT JOIN" if outer else "JOIN"} {HOTSPOTS_TABLE} h ON ST_Contains(h.geom, {point})
        {f"WHERE {where}" if where else ""}
    """
