"""``h3r{res}_crime_counts`` / ``hotspots_crime_counts`` — crimes counted per cell / crime type / month.

The H3 table indexes each crime's lat/lon straight to a cell — arithmetic on the coordinates, no join.
The Home Office hotspot hexes are a polygon layer, so those counts assign each crime point-in-polygon to
a hex, keyed by the same ``spatial_id`` / ``crime_type`` / ``month`` / ``count`` schema.

The ONS geography counts (``{key}_crime_counts``) used to live here too, which meant every step of the
H3 chain waited on five point-in-polygon passes that have nothing to do with H3. They are now
:mod:`.geography_counts`, on the ``ons`` grid.
"""

import duckdb

from safer_streets_tooling.transform import hotspots
from safer_streets_tooling.transform.base import H3_RESOLUTIONS, Grid, TransformStep, create_clause, h3_key, relation

# The crimes that contribute to the per-cell counts: geolocated, and not British Transport Police
# (their crimes are reported against the rail network rather than where they occurred, so they would
# distort the per-cell counts). Shared by the count queries and the conservation checks — here, in the
# BEAHIV counts and in the geography counts, which must count exactly the same crimes to be comparable —
# so they can't drift apart. It lives in `crime_locations`, the lowest module that needs it, and is
# re-exported here because this is where callers have always found it.
from safer_streets_tooling.transform.crime_locations import CRIME_FILTER

# the dataset half of this step's relation names: `{grid}_crime_counts` on every unit it is built for
DATASET = "crime_counts"


def expected_crimes(con: duckdb.DuckDBPyConnection) -> int:
    """How many crimes a count over the whole extract must conserve: the rows passing CRIME_FILTER."""
    return con.execute(f"SELECT COUNT(*) FROM crime_data WHERE {CRIME_FILTER}").fetchone()[0]  # ty:ignore[not-subscriptable]


def _count_in_polygons(con: duckdb.DuckDBPyConnection, key: str, table: str, expected: int, replace: bool) -> None:
    """Build ``{key}_crime_counts`` by assigning each filtered crime to the ``table`` polygon containing it.

    ST_Contains rather than ST_Intersects: the polygon layers tile without overlap, but a point exactly
    on a shared edge would otherwise be counted in both areas — dropping it is the safe failure mode.
    Only an upper bound can be asserted: a crime can fall *outside* every polygon (the E&W-only layers
    don't cover Northern Ireland, snapped points can sit just offshore of generalised boundaries, and
    the hotspot hexes cover only the flagged parts of the country) but must never land in more than one,
    so exceeding the input row count raises.
    """
    con.execute(f"""
        {create_clause("TABLE", f"{key}_crime_counts", replace=replace)} AS
        SELECT
            b.spatial_id,
            c.crime_type,
            c._month AS month,
            COUNT(*) AS count
        FROM (SELECT crime_type, _month, geom FROM crime_data WHERE {CRIME_FILTER}) c
        JOIN {table} b ON ST_Contains(b.geom, c.geom)
        GROUP BY b.spatial_id, c.crime_type, month;
    """)
    actual = con.execute(f"SELECT COALESCE(SUM(count), 0) FROM {key}_crime_counts").fetchone()[0]  # ty:ignore[not-subscriptable]
    if actual > expected:
        raise ValueError(
            f"{key}_crime_counts: counted {actual:,} crimes but only {expected:,} input rows passed the "
            f"filter — some crimes were counted in more than one area (overlapping boundary polygons)"
        )
    print(f"  {key}_crime_counts: {actual:,}/{expected:,} crimes fall within a boundary")


def build(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Create ``h3r{res}_crime_counts`` counting crimes per H3 cell / crime type / month.

    The cell is taken straight from the crime's lat/lon with ``h3_latlng_to_cell`` — no geometry, no
    join. British Transport Police records (``falls_within``) are excluded: their crimes are reported
    against the rail network rather than the place they occurred, so they would distort the counts.

    Every retained crime lands in exactly one H3 cell, so these counts must sum back to the number of
    input rows passing ``CRIME_FILTER``; a mismatch means the aggregation silently dropped (or
    duplicated) crimes and raises rather than emitting a skewed grid.
    """
    expected = expected_crimes(con)
    for res in H3_RESOLUTIONS:
        name = relation(h3_key(res), DATASET)
        con.execute(f"""
            {create_clause("TABLE", name, replace=replace)} AS
            SELECT
                lower(hex(h3_latlng_to_cell(latitude, longitude, {res}))) AS spatial_id,
                crime_type,
                _month AS month,
                COUNT(*) AS count
            FROM crime_data
            WHERE {CRIME_FILTER}
            GROUP BY spatial_id, crime_type, month;
        """)
        actual = con.execute(f"SELECT COALESCE(SUM(count), 0) FROM {name}").fetchone()[0]  # ty:ignore[not-subscriptable]
        if actual != expected:
            raise ValueError(
                f"{name}: counted {actual:,} crimes but {expected:,} input rows passed the "
                f"filter — the per-cell counts are not conserved (aggregation dropped or duplicated crimes)"
            )


def outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [relation(h3_key(res), DATASET) for res in H3_RESOLUTIONS]


def build_hotspots(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Create ``hotspots_crime_counts``: the same counts per Home Office hotspot hex.

    The hexes are just another non-overlapping polygon layer, so this is the geography count with the
    hotspots table as the boundary — except that most crimes fall outside the grid, since it covers only
    the hexes flagged as hotspots. No-op if the hotspots extract is absent.
    """
    if not hotspots.available(con):
        return
    _count_in_polygons(con, hotspots.HOTSPOT_UNIT.key, hotspots.HOTSPOTS_TABLE, expected_crimes(con), replace)


def hotspot_outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [relation(hotspots.HOTSPOT_UNIT.key, DATASET)] if hotspots.available(con) else []


STEP = TransformStep(
    name="crime_counts",
    build=build,
    outputs=outputs,
    grid=Grid.H3,
    description="Crimes counted per H3 cell / crime_type / month (BTP excluded).",
    extract_inputs=("crime_data",),
)
