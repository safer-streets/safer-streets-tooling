"""ONS boundary layers → one parquet per layer (``police_force_areas.parquet`` … ``output_areas_2021.parquet``).

Each ONS Open Geography Portal layer in the ``boundaries`` catalogue becomes its own dataset, named
after its final table. The id field is renamed to ``spatial_id`` (matching the H3 geography lookups in
``safer_streets_core.transforms``). The boundary GeoPackage is cached under the data directory by the
``ons_boundaries`` helpers and reused unless force_download.
"""

import requests
from safer_streets_core.database import duckdb_connector, write_geoparquet
from scripts import ons_boundaries

from safer_streets_tooling.datasets._common import raw_dir
from safer_streets_tooling.datasets.base import Dataset, ExtractContext


def _make_extract(layer_key: str, table: str):
    def extract(ctx: ExtractContext) -> None:
        info = ons_boundaries.sources()["layers"][layer_key]
        gpkg = raw_dir() / f"{info['filename']}_bng.gpkg"
        if ctx.force_download or not gpkg.exists():
            session = requests.Session()
            session.headers.update({"User-Agent": "ONS-Boundary-Downloader/1.0"})
            features, _ = ons_boundaries.fetch_all_features(layer_key, session, crs="bng")
            ons_boundaries.write_geopackage(features, gpkg, "bng")
        else:
            print(f"  Using cached {gpkg}")

        con = duckdb_connector(writeable=True)
        try:
            # ST_Read returns a geometry column named 'geom'
            con.execute(f"CREATE TABLE \"{table}\" AS SELECT * FROM ST_Read('{gpkg}');")
            con.execute(f'ALTER TABLE "{table}" RENAME COLUMN {info["id_field"]} TO spatial_id;')
            row_count = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]  # ty:ignore[not-subscriptable]
            write_geoparquet(con, f'SELECT * FROM "{table}"', ctx.parquet(table))
        finally:
            con.close()
        print(f"  {table}: {row_count:,} rows")

    return extract


def _datasets() -> tuple[Dataset, ...]:
    return tuple(
        Dataset(
            name=info["table"],
            table=info["table"],
            extract=_make_extract(layer_key, info["table"]),
            optional=False,  # the H3 geography lookups in transforms.py require every boundary table
        )
        for layer_key, info in ons_boundaries.sources()["layers"].items()
    )


DATASETS: tuple[Dataset, ...] = _datasets()
