"""Tests for the index.parquet catalogue builder (offline-safe: plain parquet, no spatial extension)."""

import pandas as pd
import pytest

from safer_streets_tooling.extract import DATASETS
from safer_streets_tooling.index import build_index
from safer_streets_tooling.transform import STEPS


def _write(path, **columns):
    pd.DataFrame(columns).to_parquet(path, index=False)


@pytest.fixture
def dirs(tmp_path):
    edir = tmp_path / "extract"
    tdir = tmp_path / "transform"
    edir.mkdir()
    tdir.mkdir()
    # an extract table carrying geometry, plus an optional source and its transform output
    _write(edir / "poi.parquet", spatial_id=["a", "b"], category=["x", "y"], geom=[b"\x00", b"\x01"])
    _write(edir / "streetlights.parquet", spatial_id=["h3"], h3_9_id=["h3"])
    _write(tdir / "crime_counts_h3_9.parquet", spatial_id=["h3"], crime_type=["x"], month=["2024-01"], count=[3])
    _write(tdir / "streetlight_counts_h3_9.parquet", spatial_id=["h3"], streetlight_count=[5])
    return edir, tdir


def test_registered_catalogue_descriptions_are_present():
    """Every registered dataset / transform step carries a non-empty description (validated at import)."""
    assert all(ds.description.strip() for ds in DATASETS)
    assert all(step.description.strip() for step in STEPS)


def test_build_index_rows_and_schema(dirs, tmp_path):
    edir, tdir = dirs
    out = tmp_path / "index.parquet"

    count = build_index(edir, tdir, out, resolutions=[9])
    assert count == 4

    idx = pd.read_parquet(out).set_index("name")
    assert list(idx.columns) == [
        "phase",
        "description",
        "n_rows",
        "n_columns",
        "has_geometry",
        "columns",
        "last_modified",
    ]

    assert idx.loc["poi", "phase"] == "extract"
    assert idx.loc["crime_counts_h3_9", "phase"] == "transform"
    assert idx.loc["poi", "n_rows"] == 2
    assert idx.loc["poi", "n_columns"] == 3
    assert idx.loc["poi", "columns"] == "spatial_id,category,geom"


def test_last_modified_is_the_parquet_mtime(dirs, tmp_path):
    """last_modified reflects the source parquet's mtime (UTC), i.e. when the table was last built."""
    import os
    from datetime import UTC, datetime

    edir, tdir = dirs
    os.utime(edir / "poi.parquet", (5_000_000.0, 5_000_000.0))
    out = tmp_path / "index.parquet"
    build_index(edir, tdir, out, resolutions=[9])
    idx = pd.read_parquet(out).set_index("name")

    assert idx.loc["poi", "last_modified"] == datetime.fromtimestamp(5_000_000.0, tz=UTC)


def test_geometry_flag_tracks_the_geom_column(dirs, tmp_path):
    edir, tdir = dirs
    out = tmp_path / "index.parquet"
    build_index(edir, tdir, out, resolutions=[9])
    idx = pd.read_parquet(out).set_index("name")

    assert bool(idx.loc["poi", "has_geometry"]) is True
    assert bool(idx.loc["crime_counts_h3_9", "has_geometry"]) is False


def test_descriptions_come_from_the_registries(dirs, tmp_path):
    """Extract rows take Dataset.description; an optional-gated transform output takes its step's."""
    edir, tdir = dirs
    out = tmp_path / "index.parquet"
    build_index(edir, tdir, out, resolutions=[9])
    idx = pd.read_parquet(out).set_index("name")

    poi_desc = next(ds.description for ds in DATASETS if ds.name == "poi")
    assert idx.loc["poi", "description"] == poi_desc
    # streetlight_counts_h3_9 is only described because its source (streetlights) is present as a view
    assert idx.loc["streetlight_counts_h3_9", "description"].strip()
