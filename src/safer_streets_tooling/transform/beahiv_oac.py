"""``bh350eef_oa21`` + ``bh350eef_oac`` — the 2021 Output Area Classification on the BEAHIV grid.

- ``bh350eef_oa21`` — one row per (cell, OA): ``spatial_id``, ``oa21cd`` and ``overlap_area``, the area
  (m²) of the OA inside the cell.
- ``bh350eef_oac`` — one row per cell: ``spatial_id``, its OAC subgroup ``code`` (the group and supergroup
  are prefixes of it, and ``oac_classification`` decodes all three), ``diversity`` and ``n_oa``, the number
  of OAs it overlaps.

Unlike the other BEAHIV steps, the cells are every cell an output area touches (~1.44m), not only the
crime cells of :data:`~.beahiv.BEAHIV_UNIT`: they come from the ``bh350eef`` extract, which tiles the
whole of E&W, so the step needs neither ``beahiv_counts`` nor its UDFs.

A cell's OAC input vector is the overlap-area-weighted mean of its OAs' ``oac_features``, and its ``code``
is that vector classified the way the published labels were made: top-down nearest centroid (nearest
supergroup, then the nearest group within it, then the nearest subgroup within that) against
``oac_centroids``. ``diversity`` uses the vectors only — no labels or centroids — as the weighted mean
pairwise distance between the cell's OA vectors, sum_ij w_i w_j |x_i - x_j|: 0 for a cell inside one OA.

The overlaps are a spatial join in SQL, like the other lookups; the classification and diversity are
vector arithmetic over 60 variables, which is plainer in numpy than as generated SQL. When ``oac`` is
present, the step first checks that classifying the OA features reproduces every published OA label
(the rule is exact with the right inputs, so any difference means the inputs are wrong).
"""

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
from scipy import sparse

from safer_streets_tooling.beahiv_grid import KEY
from safer_streets_tooling.transform.base import Grid, TransformStep, create_clause, table_exists

OVERLAPS = f"{KEY}_oa21"
OAC = f"{KEY}_oac"
INPUTS = (KEY, "output_areas_2021", "oac_features", "oac_centroids")

# rows of pairwise differences materialised at once by `cell_vectors`; each is a 60-float vector
_PAIR_CHUNK = 1_000_000


def _available(con: duckdb.DuckDBPyConnection) -> bool:
    return all(table_exists(con, t) for t in INPUTS)


def build_overlaps(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Create ``bh350eef_oa21``: the area of every OA inside every cell it touches.

    ``bh350eef`` has a row per (cell, police force), so a cell straddling two forces is joined twice with
    the same outline; ANY_VALUE keeps one of the identical areas. A cell that only touches an OA's
    boundary holds none of it and is dropped.
    """
    con.execute(f"""
        {create_clause("TABLE", OVERLAPS, replace=replace)} AS
        SELECT c.spatial_id, oa.spatial_id AS oa21cd, ANY_VALUE(ST_Area(ST_Intersection(c.geom, oa.geom))) AS overlap_area
        FROM output_areas_2021 oa
        JOIN {KEY} c ON ST_Intersects(c.geom, oa.geom)
        GROUP BY ALL
        HAVING overlap_area > 0
        ORDER BY spatial_id, oa21cd;
    """)


def nearest(X: np.ndarray, centroids: pd.DataFrame) -> np.ndarray:
    """The index label of the centroid nearest each row of X (squared Euclidean distance)."""
    C = centroids.to_numpy()
    d2 = (X**2).sum(axis=1)[:, None] - 2 * X @ C.T + (C**2).sum(axis=1)[None, :]
    return centroids.index.to_numpy()[d2.argmin(axis=1)]


def classify(X: np.ndarray, centroids: pd.DataFrame) -> np.ndarray:
    """Top-down nearest-centroid OAC subgroup code for each row of X.

    ``centroids`` holds every level, one row per code: a supergroup's code is one character (``3``), a
    group's two (``3c``) and a subgroup's three (``3c2``), each prefixed by its parent's. The Preview OAC was
    built top-down (k-means into supergroups, then each supergroup into groups, then each group into
    subgroups), so a vector is assigned the same way; a flat nearest-subgroup rule disagrees with the
    published labels for ~18% of OAs.
    """
    codes = centroids.index.str
    sup = nearest(X, centroids[codes.len() == 1])
    grp = np.empty(len(X), dtype=object)
    sub = np.empty(len(X), dtype=object)
    for s in np.unique(sup):
        m = sup == s
        grp[m] = nearest(X[m], centroids[(codes.len() == 2) & (codes[0] == s)])
    for g in np.unique(grp):
        m = grp == g
        sub[m] = nearest(X[m], centroids[(codes.len() == 3) & (codes[:2] == g)])
    return sub


def cell_vectors(cell: np.ndarray, oa: np.ndarray, area: np.ndarray, X: np.ndarray) -> tuple[np.ndarray, ...]:
    """Each cell's weighted mean OA vector, diversity and OA count.

    ``cell`` / ``oa`` / ``area`` are the (cell, OA) pairs, ``oa`` indexing rows of X, and must be sorted by
    cell. Returns ``(cells, mean, diversity, n_oa)`` with one entry per distinct cell, in sorted order.

    The weights are each pair's share of its cell's total overlap. The means are one sparse product of the
    (cell x OA) weight matrix with X, which never materialises a dense (pairs x variables) array (~1.2 GB
    nationally). For the diversity, sum_ij w_i w_j |x_i - x_j|, sorting by cell is what makes the pairs
    cheap to enumerate: the pairs within a cell are exactly the rows (r, r + k) that share a cell, so one
    vectorised pass per offset k covers every cell at once, with no per-cell loop. Unlike the variance, a
    sum of unsquared distances has no closed form in the cell's sums, so the pairs are needed; they're
    computed in chunks to bound memory.
    """
    cells, c, n_oa = np.unique(cell, return_inverse=True, return_counts=True)
    w = area / np.bincount(c, weights=area)[c]

    mean = sparse.csr_array((w, (c, oa)), shape=(len(cells), len(X))) @ X

    diversity = np.zeros(len(cells))
    for k in range(1, n_oa.max()):
        r = np.flatnonzero(c[:-k] == c[k:])
        for chunk in np.array_split(r, max(1, len(r) // _PAIR_CHUNK)):
            d = np.linalg.norm(X[oa[chunk]] - X[oa[chunk + k]], axis=1)
            diversity += np.bincount(c[chunk], weights=2 * w[chunk] * w[chunk + k] * d, minlength=len(cells))
    return cells, mean, diversity, n_oa


def _check_against_published(con: duckdb.DuckDBPyConnection, features: pd.DataFrame, centroids: pd.DataFrame) -> None:
    """Raise unless classifying the OA features reproduces every published OA label in ``oac``.

    Skipped when ``oac`` (a manual GeoDS download) hasn't been extracted.
    """
    if not table_exists(con, "oac"):
        print(f"  {OAC}: no oac table, so the published labels aren't checked")
        return
    published = con.execute("SELECT spatial_id, code FROM oac").df().set_index("spatial_id")["code"]
    published = published[published.index.isin(features.index)]
    predicted = classify(features.loc[published.index].to_numpy(), centroids)
    mismatches = int((predicted != published.to_numpy()).sum())
    if mismatches:
        raise ValueError(f"{OAC}: {mismatches:,} of {len(published):,} OAs don't reproduce their published OAC label")
    print(f"  {OAC}: all {len(published):,} published OA labels reproduced")


def build_oac(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Create ``bh350eef_oac`` from ``bh350eef_oa21``: each cell's OAC subgroup, diversity and OA count."""
    if not replace and table_exists(con, OAC):
        return
    centroids = con.execute("SELECT * FROM oac_centroids").df().set_index("code")
    features = con.execute("SELECT * FROM oac_features").df().set_index("spatial_id")[centroids.columns]
    _check_against_published(con, features, centroids)

    pairs = con.execute(f"SELECT spatial_id, oa21cd, overlap_area FROM {OVERLAPS} ORDER BY spatial_id, oa21cd").df()
    oa = features.index.get_indexer(pairs.oa21cd)
    if (oa < 0).any():
        raise ValueError(f"{OAC}: {(oa < 0).sum():,} overlaps are with OAs that have no OAC features")
    cells, mean, diversity, n_oa = cell_vectors(
        pairs.spatial_id.to_numpy(), oa, pairs.overlap_area.to_numpy(), features.to_numpy()
    )
    result = pa.table(
        {
            "spatial_id": pa.array(cells, type=pa.int64()),
            "code": pa.array(classify(mean, centroids), type=pa.string()),
            "diversity": pa.array(diversity, type=pa.float64()),
            "n_oa": pa.array(n_oa, type=pa.int32()),
        }
    )
    con.register("oac_cells", result)
    try:
        con.execute(f"{create_clause('TABLE', OAC, replace=replace)} AS SELECT * FROM oac_cells;")
    finally:
        con.unregister("oac_cells")
    print(f"  {OAC}: {result.num_rows:,} cells")


def build(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Build ``bh350eef_oa21`` then ``bh350eef_oac``; a no-op unless every input extract is present."""
    if not _available(con):
        print(f"  [beahiv_oac] needs {', '.join(INPUTS)} — skipping")
        return
    build_overlaps(con, replace)
    build_oac(con, replace)


def outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [OVERLAPS, OAC] if _available(con) else []


STEP = TransformStep(
    name="beahiv_oac",
    build=build,
    outputs=outputs,
    grid=Grid.BEAHIV,
    description="OAC on every BEAHIV cell an OA touches: the cell x OA overlaps, and each cell's OAC subgroup "
    "(area-weighted mean of its OAs' features, classified top-down), diversity and OA count.",
    extract_inputs=(*INPUTS, "oac"),
)
