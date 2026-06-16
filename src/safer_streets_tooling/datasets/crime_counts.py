"""police.uk street-level crime archive → ``crime_data.parquet``."""

from collections.abc import Callable

from safer_streets_core.database import duckdb_connector
from safer_streets_core.utils import data_dir

from safer_streets_tooling.datasets._common import read_geoparquet, write_geoparquet
from safer_streets_tooling.datasets.base import Dataset, ExtractContext

H3_RESOLUTIONS = [8, 9, 10]


def _make_extract(size: int) -> Callable[[ExtractContext], None]:
    def extract(ctx: ExtractContext) -> None:
        """
        TODO
        """
        con = duckdb_connector(writeable=True)
        try:
            # limited support for **/ glob, but ????-?? is a reasonable workaround
            con.execute(f"""
                CREATE TABLE crime_counts_h3_{size} AS (
                WITH c AS (
                    {read_geoparquet(data_dir() / "build/crime_data.parquet")}
                )
                SELECT
                    lower(hex(h3_latlng_to_cell(latitude, longitude, {size}))) AS spatial_id,
                    c.crime_type,
                    c._month AS month,
                    COUNT(*) AS count
                FROM c
                WHERE c.latitude IS NOT NULL AND c.longitude IS NOT NULL AND c.falls_within != 'British Transport Police'
                GROUP BY
                    spatial_id, c._month, c.crime_type
                )
            """)
            row_count = con.execute(f"SELECT COUNT(*) FROM crime_counts_h3_{size}").fetchone()[0]  # ty:ignore[not-subscriptable]
            print(f"  crime_counts: {row_count:,} rows")

            write_geoparquet(con, f"SELECT * FROM crime_counts_h3_{size}", ctx.parquet(f"crime_counts_h3_{size}"))
        finally:
            con.close()

    return extract


def _datasets() -> tuple[Dataset, ...]:
    return tuple(
        Dataset(
            name=f"crime_counts_h3_{size}",
            table=f"crime_counts_h3_{size}",
            extract=_make_extract(size),
            optional=False,
        )
        for size in H3_RESOLUTIONS
    )


DATASETS = _datasets()
