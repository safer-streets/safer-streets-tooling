"""``crime_counts_h3_{res}`` / ``crime_counts_{key}`` / ``crime_counts_hotspots`` — crimes counted per spatial unit / crime type / month.

The H3 tables index each crime's lat/lon straight to a cell; the polygon tables (the ONS geographies —
PFA / LAD / MSOA / LSOA / OA — and the Home Office hotspot hexes) assign each crime point-in-polygon to
a boundary, keyed by the same ``spatial_id`` / ``crime_type`` / ``month`` / ``count`` schema.
"""

import duckdb

from safer_streets_tooling.transform import hotspots
from safer_streets_tooling.transform.base import TransformStep, create_clause
from safer_streets_tooling.transform.geo_lookups import GEOGRAPHY_MAPPINGS

# The crimes that contribute to the per-cell counts: geolocated, and not British Transport Police
# (their crimes are reported against the rail network rather than where they occurred, so they would
# distort the per-cell counts). Shared by the count queries and the conservation checks so they can't
# drift apart.
_CRIME_FILTER = "latitude IS NOT NULL AND longitude IS NOT NULL AND falls_within != 'British Transport Police'"


def _expected(con: duckdb.DuckDBPyConnection) -> int:
    return con.execute(f"SELECT COUNT(*) FROM crime_data WHERE {_CRIME_FILTER}").fetchone()[0]  # ty:ignore[not-subscriptable]


def _count_in_polygons(con: duckdb.DuckDBPyConnection, key: str, table: str, expected: int, replace: bool) -> None:
    """Build ``crime_counts_{key}`` by assigning each filtered crime to the ``table`` polygon containing it.

    ST_Contains rather than ST_Intersects: the polygon layers tile without overlap, but a point exactly
    on a shared edge would otherwise be counted in both areas — dropping it is the safe failure mode.
    Only an upper bound can be asserted: a crime can fall *outside* every polygon (the E&W-only layers
    don't cover Northern Ireland, snapped points can sit just offshore of generalised boundaries, and
    the hotspot hexes cover only the flagged parts of the country) but must never land in more than one,
    so exceeding the input row count raises.
    """
    con.execute(f"""
        {create_clause("TABLE", f"crime_counts_{key}", replace=replace)} AS
        SELECT
            b.spatial_id,
            c.crime_type,
            c._month AS month,
            COUNT(*) AS count
        FROM (SELECT crime_type, _month, geom FROM crime_data WHERE {_CRIME_FILTER}) c
        JOIN {table} b ON ST_Contains(b.geom, c.geom)
        GROUP BY b.spatial_id, c.crime_type, month;
    """)
    actual = con.execute(f"SELECT COALESCE(SUM(count), 0) FROM crime_counts_{key}").fetchone()[0]  # ty:ignore[not-subscriptable]
    if actual > expected:
        raise ValueError(
            f"crime_counts_{key}: counted {actual:,} crimes but only {expected:,} input rows passed the "
            f"filter — some crimes were counted in more than one area (overlapping boundary polygons)"
        )
    print(f"  crime_counts_{key}: {actual:,}/{expected:,} crimes fall within a boundary")


def build(con: duckdb.DuckDBPyConnection, resolutions: list[int], replace: bool) -> None:
    """Create ``crime_counts_h3_{res}`` and ``crime_counts_{key}`` counting crimes per spatial unit /
    crime type / month.

    The spatial unit is an H3 cell (its canonical lowercase-hex string) for ``crime_counts_h3_{res}``,
    and an ONS geography code (PFA / LAD / MSOA / LSOA / OA) for ``crime_counts_{key}`` — the latter by
    joining each crime's BNG point into the boundary table with ``ST_Contains``. British Transport
    Police records (``falls_within``) are excluded from both: their crimes are reported against the
    rail network rather than the place they occurred, so they would distort the counts.

    Every retained crime lands in exactly one H3 cell, so those counts must sum back to the number of
    input rows passing ``_CRIME_FILTER``; a mismatch means the aggregation silently dropped (or
    duplicated) crimes and raises rather than emitting a skewed grid. The geography counts can only
    assert an upper bound (see :func:`_count_in_polygons`).
    """
    expected = _expected(con)
    for res in resolutions:
        con.execute(f"""
            {create_clause("TABLE", f"crime_counts_h3_{res}", replace=replace)} AS
            SELECT
                lower(hex(h3_latlng_to_cell(latitude, longitude, {res}))) AS spatial_id,
                crime_type,
                _month AS month,
                COUNT(*) AS count
            FROM crime_data
            WHERE {_CRIME_FILTER}
            GROUP BY spatial_id, crime_type, month;
        """)
        actual = con.execute(f"SELECT COALESCE(SUM(count), 0) FROM crime_counts_h3_{res}").fetchone()[0]  # ty:ignore[not-subscriptable]
        if actual != expected:
            raise ValueError(
                f"crime_counts_h3_{res}: counted {actual:,} crimes but {expected:,} input rows passed the "
                f"filter — the per-cell counts are not conserved (aggregation dropped or duplicated crimes)"
            )

    for key, table in GEOGRAPHY_MAPPINGS.items():
        _count_in_polygons(con, key, table, expected, replace)


def outputs(con: duckdb.DuckDBPyConnection, resolutions: list[int]) -> list[str]:
    return [f"crime_counts_h3_{res}" for res in resolutions] + [f"crime_counts_{key}" for key in GEOGRAPHY_MAPPINGS]


def build_hotspots(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Create ``crime_counts_hotspots``: the same counts per Home Office hotspot hex.

    The hexes are just another non-overlapping polygon layer, so this is the geography count with the
    hotspots table as the boundary — except that most crimes fall outside the grid, since it covers only
    the hexes flagged as hotspots. No-op if the hotspots extract is absent.
    """
    if not hotspots.available(con):
        return
    _count_in_polygons(con, hotspots.HOTSPOT_UNIT.key, hotspots.HOTSPOTS_TABLE, _expected(con), replace)


def hotspot_outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [f"crime_counts_{hotspots.HOTSPOT_UNIT.key}"] if hotspots.available(con) else []


STEP = TransformStep(
    name="crime_counts",
    build=build,
    outputs=outputs,
    description="Crimes counted per spatial unit (H3 cell / ONS geography code) / crime_type / month (BTP excluded).",
    extract_inputs=("crime_data", *GEOGRAPHY_MAPPINGS.values()),
)
