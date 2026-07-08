"""``crime_counts_h3_{res}`` — crimes counted per H3 cell / crime type / month."""

import duckdb

from safer_streets_tooling.transform.base import TransformStep, create_clause

# The crimes that contribute to the per-cell counts: geolocated, and not British Transport Police
# (their crimes are reported against the rail network rather than where they occurred, so they would
# distort the per-cell counts). Shared by the count query and the conservation check so the two can't
# drift apart.
_CRIME_FILTER = "latitude IS NOT NULL AND longitude IS NOT NULL AND falls_within != 'British Transport Police'"


def build(con: duckdb.DuckDBPyConnection, resolutions: list[int], replace: bool) -> None:
    """Create ``crime_counts_h3_{res}`` counting crimes per H3 cell / crime type / month.

    The H3 cell index is stored as its canonical lowercase-hex string in ``spatial_id``. British
    Transport Police records (``falls_within``) are excluded: their crimes are reported against the
    rail network rather than the place they occurred, so they would distort the per-cell counts.

    Every retained crime lands in exactly one cell, so the counts must sum back to the number of input
    rows passing ``_CRIME_FILTER``. That conservation is asserted per resolution — a mismatch means the
    aggregation silently dropped (or duplicated) crimes and raises rather than emitting a skewed grid.
    """
    expected = con.execute(f"SELECT COUNT(*) FROM crime_data WHERE {_CRIME_FILTER}").fetchone()[0]  # ty:ignore[not-subscriptable]
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


def outputs(con: duckdb.DuckDBPyConnection, resolutions: list[int]) -> list[str]:
    return [f"crime_counts_h3_{res}" for res in resolutions]


STEP = TransformStep(
    name="crime_counts",
    build=build,
    outputs=outputs,
    description="Crimes counted per H3 cell / crime_type / month (BTP excluded), keyed by spatial_id.",
    extract_inputs=("crime_data",),
)
