"""``crime_locations`` — the distinct snapped points the crime extract places crimes at.

police.uk publishes street-level crime against a fixed set of *anonymised* map points ("On or near
<street>"), so 17.5M crimes sit on ~744k distinct coordinates — about 23 crimes per point. Any
point-in-polygon assignment of crimes is therefore doing every containment test ~23 times over; doing
it once per distinct location and joining the answer back is the same result for ~4% of the work.

The table is a build intermediate (in memory, no parquet) and is idempotent: :func:`ensure` is called by
each step that needs it, in the manner of :func:`safer_streets_tooling.transform.beahiv.register_udfs`,
so no step has to depend on another purely to have it built.
"""

import duckdb

from safer_streets_tooling.transform.base import table_exists

TABLE = "crime_locations"

# The crimes that contribute to the counts: geolocated, and not British Transport Police (their crimes
# are reported against the rail network rather than where they occurred). Defined here rather than
# imported from crime_counts because that module imports this one.
CRIME_FILTER = "latitude IS NOT NULL AND longitude IS NOT NULL AND falls_within != 'British Transport Police'"

# The columns a crime row joins back to its location on. They are the source columns copied verbatim —
# never a computed value — so the doubles compare bit-for-bit and the equi-join is exact.
JOIN_KEYS = ("longitude", "latitude")


def ensure(con: duckdb.DuckDBPyConnection) -> None:
    """Create ``crime_locations`` (one row per distinct snapped coordinate) unless it already exists."""
    if table_exists(con, TABLE):
        return
    con.execute(f"""
        CREATE TABLE {TABLE} AS
        SELECT DISTINCT longitude, latitude, geom
        FROM crime_data
        WHERE {CRIME_FILTER};
    """)


def placed_in(con: duckdb.DuckDBPyConnection, table: str, code: str, source: str | None = None) -> str:
    """SQL for ``(longitude, latitude, {code})``: each distinct location assigned to its ``table`` polygon.

    ``ST_Contains`` rather than ``ST_Intersects``: the polygon layers tile without overlap, but a point
    exactly on a shared edge would otherwise be assigned twice — dropping it is the safe failure mode.
    A location outside every polygon (offshore of a generalised coastline, or outside the layer's
    coverage) simply has no row, so the caller can only assert an upper bound on the counts.

    ``source`` overrides the set of locations to place — any relation with the same columns, e.g. the
    subset a finer layer failed to cover.
    """
    ensure(con)
    return f"""
        SELECT l.longitude, l.latitude, b.spatial_id AS {code}
        FROM {source or TABLE} l
        JOIN {table} b ON ST_Contains(b.geom, l.geom)
    """
