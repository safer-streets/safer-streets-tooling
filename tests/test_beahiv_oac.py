"""Tests for the ``beahiv_oac`` transform step: ``bh350eef_oa21`` (cell x OA overlaps) and ``bh350eef_oac``.

Synthetic fixtures only — offline-safe. Two square OAs sit side by side; the ``bh350eef`` extract is
stood in for by beahiv's own cells covering them. Tests that need DuckDB's spatial extension skip when it
can't be fetched.
"""

import beahiv as bh
import duckdb
import numpy as np
import pandas as pd
import pytest
import shapely
from safer_streets_core.database import duckdb_connector, index_geometry_tables

from safer_streets_tooling.beahiv_grid import CELL_AREA, KEY, MEASURE, ORIENTATION, SIZE
from safer_streets_tooling.transform import BY_NAME, STEPS, beahiv_oac
from safer_streets_tooling.transform.base import Grid

ORIGIN = (400_000, 300_000)  # somewhere in England, in BNG metres


def _square(x: float, y: float, size: float) -> shapely.Polygon:
    return shapely.box(ORIGIN[0] + x, ORIGIN[1] + y, ORIGIN[0] + x + size, ORIGIN[1] + y + size)


OAS = {"E00000001": _square(0, 0, 1000), "E00000002": _square(1000, 0, 700)}

# two supergroups on a line, one group each; subgroup 1a1 sits *behind* supergroup 1 and 2a1 just past
# the midpoint, so a point at x=4 is nearer supergroup 1 but nearer subgroup 2a1
CENTROIDS = pd.DataFrame(
    {"v01": [0.0, 0.0, -3.0, 10.0, 10.0, 6.0], "v02": [0.0] * 6},
    index=pd.Index(["1", "1a", "1a1", "2", "2a", "2a1"], name="code"),
)
FEATURES = {"E00000001": [-2.0, 0.0], "E00000002": [9.0, 0.0], "W00000003": [7.0, 0.0]}


def test_registered_on_the_beahiv_grid():
    step = BY_NAME["beahiv_oac"]
    assert step.grid is Grid.BEAHIV
    assert step.depends_on == ()
    assert {KEY, "output_areas_2021", "oac_features", "oac_centroids", "oac"} == set(step.extract_inputs)
    assert step in STEPS


# --- classify -------------------------------------------------------------------------------------


def test_classify_is_top_down_not_flat():
    X = np.array([[4.0, 0.0]])
    assert beahiv_oac.nearest(X, CENTROIDS.loc[["1a1", "2a1"]])[0] == "2a1"  # the flat rule
    assert beahiv_oac.classify(X, CENTROIDS)[0] == "1a1"


def test_classify_only_considers_children_of_the_chosen_parent():
    centroids = pd.DataFrame(
        {"v01": [0.0, -1.0, 1.0, -1.0, -2.0, 1.0, 2.0, 10.0, 10.0, 10.0]},
        index=["1", "1a", "1b", "1a1", "1a2", "1b1", "1b2", "2", "2a", "2a1"],
    )
    X = np.array([[1.9], [-1.1], [9.0]])
    assert list(beahiv_oac.classify(X, centroids)) == ["1b2", "1a1", "2a1"]


# --- cell_vectors ---------------------------------------------------------------------------------


def test_cell_vectors_hand_worked():
    # cell 10: OAs at (0,0) and (3,4), 5 apart, overlaps 1:3; cell 20: one OA
    X = np.array([[0.0, 0.0], [3.0, 4.0], [7.0, 7.0]])
    cells, mean, diversity, n_oa = beahiv_oac.cell_vectors(
        np.array([10, 10, 20]), np.array([0, 1, 2]), np.array([1.0, 3.0, 50.0]), X
    )
    assert list(cells) == [10, 20]
    assert list(n_oa) == [2, 1]
    np.testing.assert_allclose(mean, [[2.25, 3.0], [7.0, 7.0]])
    # sum over ordered pairs i != j of w_i w_j d_ij = 2 * 0.25 * 0.75 * 5
    np.testing.assert_allclose(diversity, [1.875, 0.0])


def test_cell_vectors_matches_brute_force_across_chunks(monkeypatch):
    monkeypatch.setattr(beahiv_oac, "_PAIR_CHUNK", 3)  # force several chunks per offset
    rng = np.random.default_rng(0)
    X = rng.random((40, 5))
    sizes = rng.integers(1, 9, 30)
    cell = np.repeat(np.arange(30) * 7, sizes)
    oa = rng.integers(0, len(X), len(cell))
    area = rng.random(len(cell)) + 0.1
    cells, mean, diversity, n_oa = beahiv_oac.cell_vectors(cell, oa, area, X)
    assert list(n_oa) == list(sizes)
    for i, c in enumerate(cells):
        rows = cell == c
        w = area[rows] / area[rows].sum()
        V = X[oa[rows]]
        np.testing.assert_allclose(mean[i], w @ V)
        np.testing.assert_allclose(diversity[i], w @ np.linalg.norm(V[:, None] - V[None], axis=-1) @ w)


# --- the step -------------------------------------------------------------------------------------


def _connect(published: dict[str, str] | None) -> duckdb.DuckDBPyConnection:
    """The step's input tables as the transform imports them, with ``oac`` only when ``published`` is given."""
    try:
        con = duckdb_connector(writeable=True)
    except duckdb.HTTPException as e:  # extension download unavailable
        pytest.skip(f"extension download unavailable: {e}")
    cell_ids = np.asarray(
        bh.polyfill(shapely.union_all(list(OAS.values())), size=SIZE, measure=MEASURE, orientation=ORIENTATION),
        dtype=np.int64,
    )
    # full precision, as the extract writes them: rounded WKT would enlarge a cell past CELL_AREA
    wkt = shapely.to_wkt(np.asarray(bh.cell_polygons(cell_ids)), rounding_precision=-1)
    cells = pd.DataFrame({"spatial_id": cell_ids, "wkt": wkt})
    # the extract has a row per (cell, police force): repeat a few cells as if they straddled two forces
    con.register("cells", pd.concat([cells, cells.iloc[:3]]))
    con.execute(f"CREATE TABLE {KEY} AS SELECT spatial_id, ST_GeomFromText(wkt) AS geom FROM cells")
    con.register("oas", pd.DataFrame({"spatial_id": list(OAS), "wkt": [g.wkt for g in OAS.values()]}))
    con.execute("CREATE TABLE output_areas_2021 AS SELECT spatial_id, ST_GeomFromText(wkt) AS geom FROM oas")
    con.register("features", pd.DataFrame(FEATURES, index=["v01", "v02"]).T.rename_axis("spatial_id").reset_index())
    con.execute("CREATE TABLE oac_features AS SELECT * FROM features")
    con.register("centroids", CENTROIDS.reset_index())
    con.execute("CREATE TABLE oac_centroids AS SELECT * FROM centroids")
    if published is not None:
        con.register("labels", pd.DataFrame({"spatial_id": list(published), "code": list(published.values())}))
        con.execute("CREATE TABLE oac AS SELECT * FROM labels")
    index_geometry_tables(con)  # as the transform does on import: repaired, labelled BNG, RTree-indexed
    return con


# what classify gives for each OA's features, so the published-label check passes
PUBLISHED = {"E00000001": "1a1", "E00000002": "2a1", "W00000003": "2a1"}


@pytest.fixture
def built() -> tuple[pd.DataFrame, pd.DataFrame]:
    con = _connect(PUBLISHED)
    beahiv_oac.build(con, True)
    assert beahiv_oac.outputs(con) == [beahiv_oac.OVERLAPS, beahiv_oac.OAC]
    pairs = con.execute(f"SELECT * FROM {beahiv_oac.OVERLAPS}").df()
    cells = con.execute(f"SELECT * FROM {beahiv_oac.OAC}").df().set_index("spatial_id")
    return pairs, cells


def test_overlaps_partition_each_oa(built):
    pairs, _ = built
    assert list(pairs.columns) == ["spatial_id", "oa21cd", "overlap_area"]
    assert not pairs.duplicated(["spatial_id", "oa21cd"]).any()  # the repeated cells count once
    assert (pairs.overlap_area > 0).all()
    assert (pairs.overlap_area <= CELL_AREA * (1 + 1e-9)).all()
    for code, geom in OAS.items():
        assert pairs.loc[pairs.oa21cd == code, "overlap_area"].sum() == pytest.approx(geom.area)


def test_oac_per_cell(built):
    pairs, cells = built
    assert list(cells.columns) == ["code", "diversity", "n_oa"]
    assert set(cells.index) == set(pairs.spatial_id)
    assert (cells.loc[cells.n_oa == 1, "diversity"] == 0).all()

    sole = pairs.groupby("spatial_id").oa21cd.agg(lambda s: s.iloc[0] if len(s) == 1 else None).dropna()
    assert (cells.loc[sole[sole == "E00000001"].index, "code"] == "1a1").all()
    assert (cells.loc[sole[sole == "E00000002"].index, "code"] == "2a1").all()

    # a cell straddling both OAs: diversity is w1 w2 |x1 - x2| over both orders, and the mean decides the code
    shared = cells[cells.n_oa == 2]
    assert len(shared) > 0
    x1, x2 = np.array(FEATURES["E00000001"]), np.array(FEATURES["E00000002"])
    for cell, row in shared.iterrows():
        a = pairs[pairs.spatial_id == cell].set_index("oa21cd").overlap_area
        w1, w2 = a["E00000001"] / a.sum(), a["E00000002"] / a.sum()
        assert row.diversity == pytest.approx(2 * w1 * w2 * np.linalg.norm(x1 - x2))
        assert row.code == beahiv_oac.classify((w1 * x1 + w2 * x2)[None], CENTROIDS)[0]


def test_without_published_labels_the_check_is_skipped():
    con = _connect(None)
    beahiv_oac.build(con, True)
    assert con.execute(f"SELECT COUNT(*) FROM {beahiv_oac.OAC}").fetchone()[0] > 0  # ty:ignore[not-subscriptable]


def test_fails_when_published_labels_are_not_reproduced():
    con = _connect({"E00000001": "2a1", "E00000002": "2a1"})
    with pytest.raises(ValueError, match="1 of 2 OAs"):
        beahiv_oac.build(con, True)


def test_skipped_without_its_inputs():
    try:
        con = duckdb_connector(writeable=True)
    except duckdb.HTTPException as e:
        pytest.skip(f"extension download unavailable: {e}")
    beahiv_oac.build(con, True)
    assert beahiv_oac.outputs(con) == []
