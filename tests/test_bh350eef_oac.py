"""Tests for the OAC-on-BEAHIV extract: ``bh350eef_oa21`` (cell x OA overlaps) and ``bh350eef_oac``.

Offline-safe: the OA vectors and centroids are synthesised into a temporary raw folder under the file
names the catalogue's URLs end in, so the cached-download path never reaches the network; the boundary
parquet is synthesised too. Tests that write GeoParquet skip when DuckDB cannot fetch its spatial
extension.
"""

import duckdb
import numpy as np
import pandas as pd
import pytest
import shapely
from safer_streets_core.database import duckdb_connector, write_geoparquet

from safer_streets_tooling.beahiv_grid import CELL_AREA
from safer_streets_tooling.config import data_source
from safer_streets_tooling.extract import DATASETS
from safer_streets_tooling.extract import bh350eef_oac as m
from safer_streets_tooling.extract.base import ExtractContext

ORIGIN = (400_000, 300_000)  # somewhere in England, in BNG metres


def _square(x: float, y: float, size: float) -> shapely.Polygon:
    return shapely.box(ORIGIN[0] + x, ORIGIN[1] + y, ORIGIN[0] + x + size, ORIGIN[1] + y + size)


def test_registered_after_their_dependencies():
    names = [ds.name for ds in DATASETS]
    assert names.index("output_areas_2021") < names.index(m.OVERLAPS)
    assert names.index("oac") < names.index(m.OAC)
    assert names.index(m.OVERLAPS) < names.index(m.OAC)


# --- classify -------------------------------------------------------------------------------------

# two supergroups on a line, one group each; subgroup 1a1 sits *behind* supergroup 1 and 2a1 just past
# the midpoint, so a point at x=4 is nearer supergroup 1 but nearer subgroup 2a1
SUPERGROUPS = pd.DataFrame({"x": [0.0, 10.0], "y": [0.0, 0.0]}, index=["1", "2"])
GROUPS = pd.DataFrame({"x": [0.0, 10.0], "y": [0.0, 0.0]}, index=["1a", "2a"])
SUBGROUPS = pd.DataFrame({"x": [-3.0, 6.0], "y": [0.0, 0.0]}, index=["1a1", "2a1"])


def test_classify_is_top_down_not_flat():
    X = np.array([[4.0, 0.0]])
    assert m.nearest(X, SUBGROUPS)[0] == "2a1"  # the flat rule
    assert m.classify(X, SUPERGROUPS, GROUPS, SUBGROUPS)[0] == "1a1"


def test_classify_only_considers_children_of_the_chosen_parent():
    groups = pd.DataFrame({"x": [-1.0, 1.0, 10.0], "y": [0.0, 0.0, 0.0]}, index=["1a", "1b", "2a"])
    subgroups = pd.DataFrame(
        {"x": [-1.0, -2.0, 1.0, 2.0, 10.0], "y": [0.0, 0.0, 0.0, 0.0, 0.0]},
        index=["1a1", "1a2", "1b1", "1b2", "2a1"],
    )
    X = np.array([[1.9, 0.0], [-1.1, 0.0], [9.0, 0.0]])
    assert list(m.classify(X, SUPERGROUPS, groups, subgroups)) == ["1b2", "1a1", "2a1"]


# --- cell_vectors ---------------------------------------------------------------------------------


def _brute_force(cell, oa, area, X):
    out = {}
    for c in np.unique(cell):
        rows = cell == c
        w = area[rows] / area[rows].sum()
        V = X[oa[rows]]
        D = np.linalg.norm(V[:, None] - V[None], axis=-1)
        out[c] = (w @ V, w @ D @ w)
    return out


def test_cell_vectors_hand_worked():
    # cell 10: OAs at (0,0) and (3,4), 5 apart, overlaps 1:3; cell 20: one OA
    X = np.array([[0.0, 0.0], [3.0, 4.0], [7.0, 7.0]])
    cells, mean, diversity, n_oa = m.cell_vectors(
        np.array([10, 10, 20]), np.array([0, 1, 2]), np.array([1.0, 3.0, 50.0]), X
    )
    assert list(cells) == [10, 20]
    assert list(n_oa) == [2, 1]
    np.testing.assert_allclose(mean, [[2.25, 3.0], [7.0, 7.0]])
    # sum over ordered pairs i != j of w_i w_j d_ij = 2 * 0.25 * 0.75 * 5
    np.testing.assert_allclose(diversity, [1.875, 0.0])


def test_cell_vectors_matches_brute_force_across_chunks(monkeypatch):
    monkeypatch.setattr(m, "_PAIR_CHUNK", 3)  # force several chunks per offset
    rng = np.random.default_rng(0)
    X = rng.random((40, 5))
    sizes = rng.integers(1, 9, 30)
    cell = np.repeat(np.arange(30) * 7, sizes)
    oa = rng.integers(0, len(X), len(cell))
    area = rng.random(len(cell)) + 0.1
    cells, mean, diversity, n_oa = m.cell_vectors(cell, oa, area, X)
    expected = _brute_force(cell, oa, area, X)
    assert list(n_oa) == list(sizes)
    for i, c in enumerate(cells):
        np.testing.assert_allclose(mean[i], expected[c][0])
        np.testing.assert_allclose(diversity[i], expected[c][1])


# --- oa_cell_overlaps -----------------------------------------------------------------------------


def test_overlaps_partition_each_oa():
    oas = {"E00000001": _square(0, 0, 1000), "E00000002": _square(1000, 0, 700)}
    t = m.oa_cell_overlaps(list(oas), [shapely.to_wkb(g) for g in oas.values()]).to_pandas()
    assert (t.overlap_area > 0).all()
    assert (t.overlap_area <= CELL_AREA * (1 + 1e-9)).all()
    for code, geom in oas.items():
        assert t.loc[t.oa21cd == code, "overlap_area"].sum() == pytest.approx(geom.area)
    # the two OAs share an edge, so the cells along it appear once per OA
    assert t.groupby("spatial_id").oa21cd.nunique().max() == 2


def test_overlaps_repair_invalid_boundaries():
    bowtie = shapely.Polygon([(0, 0), (1000, 1000), (1000, 0), (0, 1000)])
    bowtie = shapely.affinity.translate(bowtie, *ORIGIN)
    assert not bowtie.is_valid
    t = m.oa_cell_overlaps(["E00000003"], [shapely.to_wkb(bowtie)]).to_pandas()
    assert t.overlap_area.sum() == pytest.approx(shapely.make_valid(bowtie).area)


# --- the two extracts end to end ------------------------------------------------------------------

VARIABLES = ["v01", "v02"]
OA_VECTORS = {"E00000001": [-2.0, 0.0], "E00000002": [9.0, 0.0], "W00000003": [7.0, 0.0]}


def _write_inputs(tmp_path, monkeypatch, published: dict[str, str] | None):
    """Synthesise the boundaries (two squares), the cached downloads and, optionally, ``oac``."""
    staging, raw = tmp_path / "staging", tmp_path / "raw"
    staging.mkdir()
    raw.mkdir()
    try:
        con = duckdb_connector(writeable=True)
    except duckdb.HTTPException:
        pytest.skip("spatial extension unavailable")
    try:
        con.execute("CREATE TABLE oa (spatial_id VARCHAR, geom GEOMETRY);")
        con.executemany(
            "INSERT INTO oa VALUES (?, ST_GeomFromText(?::VARCHAR))",
            [("E00000001", _square(0, 0, 1000).wkt), ("E00000002", _square(1000, 0, 700).wkt)],
        )
        write_geoparquet(con, "SELECT * FROM oa", staging / "output_areas_2021.parquet")
    finally:
        con.close()

    src = data_source("oac_vectors")
    name = lambda key: src[key].rsplit("/", 1)[-1]  # noqa: E731
    vectors = pd.DataFrame(OA_VECTORS, index=VARIABLES).T.rename_axis("OA").reset_index()
    # a Scotland row with a missing variable must be ignored
    vectors.loc[len(vectors)] = ["S00000004", 1.0, np.nan]
    vectors.to_parquet(raw / name("url"))
    SUPERGROUPS.set_axis(VARIABLES, axis=1).rename_axis("Supergroup8").reset_index().to_csv(
        raw / name("supergroup_centroids_url"), index=False
    )
    GROUPS.set_axis(VARIABLES, axis=1).rename_axis("Group").reset_index().to_csv(
        raw / name("group_centroids_url"), index=False
    )
    SUBGROUPS.set_axis(VARIABLES, axis=1).rename_axis("Subgroup").reset_index().to_csv(
        raw / name("subgroup_centroids_url"), index=False
    )
    if published is not None:
        pd.DataFrame({"spatial_id": list(published), "code": list(published.values())}).to_parquet(
            staging / "oac.parquet"
        )
    monkeypatch.setattr(m, "raw_dir", lambda: raw)
    monkeypatch.setattr(m, "download", lambda url, dest: pytest.fail(f"tried to download {url}"))
    return ExtractContext(staging=staging)


def test_extract_end_to_end(tmp_path, monkeypatch):
    # what classify gives for each OA vector, so the published-label check passes
    ctx = _write_inputs(tmp_path, monkeypatch, {"E00000001": "1a1", "E00000002": "2a1", "W00000003": "2a1"})
    m.extract_overlaps(ctx)
    m.extract_oac(ctx)

    pairs = pd.read_parquet(ctx.parquet(m.OVERLAPS))
    out = pd.read_parquet(ctx.parquet(m.OAC)).set_index("spatial_id")
    assert list(out.columns) == ["code", "diversity", "n_oa"]
    assert set(out.index) == set(pairs.spatial_id)

    single = out[out.n_oa == 1]
    assert (single.diversity == 0).all()
    # a cell wholly inside OA 1 takes OA 1's label; one wholly inside OA 2 takes OA 2's
    inside = pairs.groupby("spatial_id").oa21cd.agg(lambda s: s.iloc[0] if len(s) == 1 else None).dropna()
    assert (out.loc[inside[inside == "E00000001"].index, "code"] == "1a1").all()
    assert (out.loc[inside[inside == "E00000002"].index, "code"] == "2a1").all()

    # a straddling cell: diversity is w1 w2 |x1 - x2| over both orders, and the mean decides the label
    shared = out[out.n_oa == 2]
    assert len(shared) > 0
    for cell, row in shared.iterrows():
        a = pairs[pairs.spatial_id == cell].set_index("oa21cd").overlap_area
        w = a / a.sum()
        assert row.diversity == pytest.approx(2 * w["E00000001"] * w["E00000002"] * 11.0)
        mean = w["E00000001"] * np.array(OA_VECTORS["E00000001"]) + w["E00000002"] * np.array(OA_VECTORS["E00000002"])
        assert row.code == m.classify(mean[None], SUPERGROUPS, GROUPS, SUBGROUPS)[0]


def test_extract_without_published_labels_skips_the_check(tmp_path, monkeypatch):
    ctx = _write_inputs(tmp_path, monkeypatch, None)
    m.extract_overlaps(ctx)
    m.extract_oac(ctx)
    assert ctx.parquet(m.OAC).exists()


def test_extract_fails_when_published_labels_are_not_reproduced(tmp_path, monkeypatch):
    ctx = _write_inputs(tmp_path, monkeypatch, {"E00000001": "2a1", "E00000002": "2a1"})
    m.extract_overlaps(ctx)
    with pytest.raises(ValueError, match="1 of 2 OAs"):
        m.extract_oac(ctx)


def test_extract_needs_its_inputs(tmp_path):
    ctx = ExtractContext(staging=tmp_path)
    with pytest.raises(FileNotFoundError, match="output_areas_2021"):
        m.extract_overlaps(ctx)
    with pytest.raises(FileNotFoundError, match=m.OVERLAPS):
        m.extract_oac(ctx)
