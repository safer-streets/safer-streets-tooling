"""Tests for the downloaded OAC tables: ``oac_features`` and ``oac_centroids``.

Offline-safe: the source files are synthesised into a temporary raw folder under the file names the
catalogue's URLs end in, so the cached-download path never reaches the network.
"""

import pandas as pd
import pytest

from safer_streets_tooling.config import data_source
from safer_streets_tooling.extract import BY_NAME, oac
from safer_streets_tooling.extract.base import ExtractContext


def _raw_name(key: str) -> str:
    return data_source("oac")[key].rsplit("/", 1)[-1]


@pytest.fixture
def ctx(tmp_path, monkeypatch) -> ExtractContext:
    raw, staging = tmp_path / "raw", tmp_path / "staging"
    raw.mkdir()
    staging.mkdir()
    monkeypatch.setattr(oac, "raw_dir", lambda: raw)
    monkeypatch.setattr(oac, "download", lambda url, dest: pytest.fail(f"tried to download {url}"))
    return ExtractContext(staging=staging)


def test_registered():
    assert {"oac_features", "oac_centroids"} <= set(BY_NAME)
    assert not BY_NAME["oac_features"].geometry and not BY_NAME["oac_centroids"].geometry


def test_features_keep_ew_and_order_the_variables(ctx):
    # the fork's file lists the variables out of order and includes Scotland/NI rows lacking v40
    pd.DataFrame(
        {
            "OA": ["W00000002", "S00000003", "E00000001"],
            "v02": [0.2, 0.5, 0.1],
            "v40": [0.9, None, 0.8],
            "v01": [0.3, 0.6, 0.4],
        }
    ).to_parquet(oac.raw_dir() / _raw_name("features_url"))
    oac.extract_oac_features(ctx)
    out = pd.read_parquet(ctx.parquet("oac_features"))
    assert list(out.columns) == ["spatial_id", "v01", "v02", "v40"]
    assert list(out.spatial_id) == ["E00000001", "W00000002"]
    assert out.loc[0, ["v01", "v02", "v40"]].tolist() == [0.4, 0.1, 0.8]


def test_centroids_stack_the_three_levels(ctx):
    raw = oac.raw_dir()
    pd.DataFrame({"v02": [1.0, 2.0], "v01": [3.0, 4.0], "Supergroup8": [1, 2]}).to_csv(
        raw / _raw_name("supergroup_centroids_url"), index=False
    )
    pd.DataFrame({"Group": ["1a", "2a"], "v01": [5.0, 6.0], "v02": [7.0, 8.0]}).to_csv(
        raw / _raw_name("group_centroids_url"), index=False
    )
    pd.DataFrame({"Subgroup": ["1a1", "2a1"], "v01": [9.0, 10.0], "v02": [11.0, 12.0]}).to_csv(
        raw / _raw_name("subgroup_centroids_url"), index=False
    )
    oac.extract_oac_centroids(ctx)
    out = pd.read_parquet(ctx.parquet("oac_centroids")).set_index("code")
    assert list(out.columns) == ["v01", "v02"]
    assert list(out.index) == ["1", "1a", "1a1", "2", "2a", "2a1"]
    assert out.loc["1"].tolist() == [3.0, 1.0]
    assert out.loc["2a1"].tolist() == [10.0, 12.0]
