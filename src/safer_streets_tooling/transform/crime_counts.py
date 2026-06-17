"""``crime_counts_h3_{res}`` — crimes counted per H3 cell / crime type / month."""

import duckdb

from safer_streets_tooling.transform.base import TransformStep, create_clause


def build(con: duckdb.DuckDBPyConnection, resolutions: list[int], replace: bool) -> None:
    """Create ``crime_counts_h3_{res}`` counting crimes per H3 cell / crime type / month.

    The H3 cell index is stored as its canonical lowercase-hex string in ``spatial_id``. British
    Transport Police records (``falls_within``) are excluded: their crimes are reported against the
    rail network rather than the place they occurred, so they would distort the per-cell counts.
    """
    for res in resolutions:
        con.execute(f"""
            {create_clause("TABLE", f"crime_counts_h3_{res}", replace=replace)} AS
            SELECT
                lower(hex(h3_latlng_to_cell(latitude, longitude, {res}))) AS spatial_id,
                crime_type,
                _month AS month,
                COUNT(*) AS count
            FROM crime_data
            WHERE latitude IS NOT NULL
                AND longitude IS NOT NULL
                AND falls_within != 'British Transport Police'
            GROUP BY spatial_id, crime_type, month;
        """)


def outputs(con: duckdb.DuckDBPyConnection, resolutions: list[int]) -> list[str]:
    return [f"crime_counts_h3_{res}" for res in resolutions]


STEP = TransformStep(name="crime_counts", build=build, outputs=outputs, extract_inputs=("crime_data",))
