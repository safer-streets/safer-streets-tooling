"""The 2021 Output Area Classification projected onto the BEAHIV grid → ``bh350eef_oa21`` + ``bh350eef_oac``.

Two attribute-only tables covering every cell an output area touches (~1.44m cells), not just the
cells carrying crimes the BEAHIV transform steps use — which is why this is an extract rather than a
transform step.

- ``bh350eef_oa21`` — one row per (cell, OA) pair: ``spatial_id``, ``oa21cd`` and ``overlap_area``, the
  area (m²) of the OA inside the cell. Derived from the ``output_areas_2021`` boundaries, the way
  ``bh350eef`` is derived from the force boundaries.
- ``bh350eef_oac`` — one row per cell: ``spatial_id``, its OAC subgroup ``code`` (the group and
  supergroup are prefixes of it, and ``oac_classification`` decodes all three), ``diversity`` and
  ``n_oa``, the number of OAs it overlaps.

A cell's OAC input vector is the overlap-area-weighted mean of its OAs' vectors, and its ``code`` is
that vector classified the way the published labels were made: top-down nearest centroid (nearest
supergroup, then the nearest group within it, then the nearest subgroup within that) against the
Preview OAC k-means centroids. ``diversity`` uses the vectors only — no labels or centroids — as the
weighted mean pairwise distance between the cell's OA vectors, sum_ij w_i w_j |x_i - x_j|: 0 for a cell
inside one OA.

The OA vectors are the asinh + range-standardised inputs from the safer-streets fork of the UK OAC
repository, whose E&W rows were replaced with the Preview inputs the published E&W labels were built
from (safer-streets/Output_Area_Classification#1). With them, the rule reproduces every published E&W
label; the extract checks this against ``oac`` when that (manually downloaded) table is present.
"""

from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import shapely
from safer_streets_core.database import duckdb_connector, read_geoparquet, write_geoparquet
from scipy import sparse
from shapely.geometry.base import BaseGeometry

from safer_streets_tooling.beahiv_grid import KEY
from safer_streets_tooling.config import data_source
from safer_streets_tooling.extract._common import download, raw_dir
from safer_streets_tooling.extract.base import Dataset, ExtractContext
from safer_streets_tooling.extract.bh350eef import polyfill_force

OVERLAPS = f"{KEY}_oa21"
OAC = f"{KEY}_oac"

# rows of pairwise differences materialised at once by `diversity`; each is a 60-float vector
_PAIR_CHUNK = 1_000_000


def _polygonal(geom: BaseGeometry) -> BaseGeometry:
    """``geom`` made valid, keeping only its polygonal parts (polyfill takes a Polygon/MultiPolygon).

    A few OA boundaries are invalid (self-intersections); ``make_valid`` can turn those into a collection
    holding stray lines or points, which cover no area and so contribute nothing to an overlap.
    """
    if shapely.is_valid(geom):
        return geom
    parts = shapely.get_parts(shapely.make_valid(geom))
    return shapely.union_all([p for p in parts if isinstance(p, shapely.Polygon | shapely.MultiPolygon)])


def oa_cell_overlaps(oa21cd: list[str], wkb: list[bytes]) -> pa.Table:
    """Every (cell, OA) pair with a positive overlap: ``spatial_id``, ``oa21cd``, ``overlap_area`` (m²).

    Each OA goes through the same polyfill + clip as a police force in ``bh350eef``. A cell that only
    touches an OA's boundary has a zero overlap and is dropped, since it holds none of the OA.
    """
    cells, areas, codes = [], [], []
    for code, blob in zip(oa21cd, wkb, strict=True):
        cell_ids, overlap_area, _ = polyfill_force(_polygonal(shapely.from_wkb(blob)))
        keep = overlap_area > 0
        cells.append(cell_ids[keep])
        areas.append(overlap_area[keep])
        codes.append(np.full(keep.sum(), code, dtype=object))
    return pa.table(
        {
            "spatial_id": pa.array(np.concatenate(cells), type=pa.int64()),
            "oa21cd": pa.array(np.concatenate(codes), type=pa.string()),
            "overlap_area": pa.array(np.concatenate(areas), type=pa.float64()),
        }
    )


def extract_overlaps(ctx: ExtractContext) -> None:
    """Write the ``bh350eef_oa21`` parquet from the 2021 output area boundaries."""
    oas = ctx.parquet("output_areas_2021")
    if not oas.exists():
        raise FileNotFoundError(f"output_areas_2021 parquet not found: {oas}")
    con = duckdb_connector(writeable=True)
    try:
        rows = con.execute(
            f"SELECT spatial_id, ST_AsWKB(geom) FROM ({read_geoparquet(oas)}) ORDER BY spatial_id"
        ).fetchall()
        print(f"  {OVERLAPS}: polyfilling {len(rows):,} output areas…")
        pairs = oa_cell_overlaps([r[0] for r in rows], [bytes(r[1]) for r in rows])
        con.register("pairs", pairs)
        write_geoparquet(con, "SELECT * FROM pairs ORDER BY spatial_id, oa21cd", ctx.parquet(OVERLAPS))
    finally:
        con.close()
    print(f"  {OVERLAPS}: {pairs.num_rows:,} rows")


def nearest(X: np.ndarray, centroids: pd.DataFrame) -> np.ndarray:
    """The index label of the centroid nearest each row of X (squared Euclidean distance)."""
    C = centroids.to_numpy()
    d2 = (X**2).sum(axis=1)[:, None] - 2 * X @ C.T + (C**2).sum(axis=1)[None, :]
    return centroids.index.to_numpy()[d2.argmin(axis=1)]


def classify(X: np.ndarray, supergroups: pd.DataFrame, groups: pd.DataFrame, subgroups: pd.DataFrame) -> np.ndarray:
    """Top-down nearest-centroid OAC subgroup code for each row of X.

    The Preview OAC was built top-down (k-means into supergroups, then each supergroup into groups, then
    each group into subgroups), so a vector is assigned the same way. A flat nearest-subgroup rule
    disagrees with the published labels for ~18% of OAs. Group and subgroup codes are prefixed by their
    parent's code (``3`` → ``3c`` → ``3c2``), which is how each level's candidates are found.
    """
    sup = nearest(X, supergroups)
    grp = np.empty(len(X), dtype=object)
    sub = np.empty(len(X), dtype=object)
    for s in np.unique(sup):
        m = sup == s
        grp[m] = nearest(X[m], groups[groups.index.str[0] == s])
    for g in np.unique(grp):
        m = grp == g
        sub[m] = nearest(X[m], subgroups[subgroups.index.str[:2] == g])
    return sub


def cell_vectors(cell: np.ndarray, oa: np.ndarray, area: np.ndarray, X: np.ndarray) -> tuple[np.ndarray, ...]:
    """Each cell's weighted mean OA vector, diversity and OA count.

    ``cell`` / ``oa`` / ``area`` are the (cell, OA) pairs, ``oa`` indexing rows of X, and must be sorted by
    cell. Returns ``(cells, mean, diversity, n_oa)`` with one entry per distinct cell, in sorted order.

    The weights are each pair's share of its cell's total overlap. The means are one sparse product of the
    (cell x OA) weight matrix with X, which never materialises a dense (pairs x variables) array (~1.4 GB
    nationally). For the diversity, sum_ij w_i w_j |x_i - x_j|, sorting by cell is what makes the
    pairs cheap to enumerate: the pairs within a cell are exactly the rows (r, r + k) that share a cell,
    so one vectorised pass per offset k covers every cell at once, with no per-cell loop. Unlike the
    variance, a sum of unsquared distances has no closed form in the cell's sums, so the pairs are
    needed; they're computed in chunks to bound memory.
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


def _cached(url: str, *, force_download: bool) -> Path:
    path = raw_dir() / url.rsplit("/", 1)[-1]
    if not path.exists() or force_download:
        download(url, path)
    return path


def _load_inputs(force_download: bool) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """The E&W OA vectors (indexed by oa21cd) and the supergroup, group and subgroup centroids."""
    src = data_source("oac_vectors")
    subgroups = pd.read_csv(_cached(src["subgroup_centroids_url"], force_download=force_download)).set_index("Subgroup")
    variables = list(subgroups.columns)
    groups = pd.read_csv(_cached(src["group_centroids_url"], force_download=force_download)).set_index("Group")
    supergroups = pd.read_csv(
        _cached(src["supergroup_centroids_url"], force_download=force_download), dtype={"Supergroup8": str}
    ).set_index("Supergroup8")
    vectors = pd.read_parquet(_cached(src["url"], force_download=force_download)).set_index("OA")
    # S/NI rows lack v40/v41 (they were assigned without them), and only E&W OAs are on the grid
    vectors = vectors[vectors.index.str[0].isin(["E", "W"])]
    return vectors[variables], supergroups[variables], groups[variables], subgroups[variables]


def _check_against_published(ctx: ExtractContext, vectors: pd.DataFrame, centroids: tuple[pd.DataFrame, ...]) -> None:
    """Raise unless classifying the OA vectors reproduces every published OA label in ``oac``.

    A guard against the upstream vectors or centroids changing under the pinned URLs being updated: with
    the right inputs the rule is exact, so any disagreement means the inputs are wrong. Skipped when
    ``oac`` (a manual GeoDS download) hasn't been extracted.
    """
    path = ctx.parquet("oac")
    if not path.exists():
        print(f"  {OAC}: no oac parquet, so the published labels aren't checked")
        return
    published = pd.read_parquet(path).set_index("spatial_id")["code"]
    published = published[published.index.isin(vectors.index)]
    predicted = classify(vectors.loc[published.index].to_numpy(), *centroids)
    mismatches = int((predicted != published.to_numpy()).sum())
    if mismatches:
        raise ValueError(f"{OAC}: {mismatches:,} of {len(published):,} OAs don't reproduce their published OAC label")
    print(f"  {OAC}: all {len(published):,} published OA labels reproduced")


def extract_oac(ctx: ExtractContext) -> None:
    """Write the ``bh350eef_oac`` parquet: each cell's OAC subgroup, diversity and OA count."""
    overlaps_path = ctx.parquet(OVERLAPS)
    if not overlaps_path.exists():
        raise FileNotFoundError(f"{OVERLAPS} parquet not found: {overlaps_path}")
    vectors, *centroids = _load_inputs(ctx.force_download)
    _check_against_published(ctx, vectors, tuple(centroids))

    pairs = pd.read_parquet(overlaps_path).sort_values(["spatial_id", "oa21cd"])
    oa = vectors.index.get_indexer(pairs.oa21cd)
    if (oa < 0).any():
        raise ValueError(f"{OAC}: {(oa < 0).sum():,} overlaps are with OAs that have no OAC input vector")
    cells, mean, diversity, n_oa = cell_vectors(
        pairs.spatial_id.to_numpy(), oa, pairs.overlap_area.to_numpy(), vectors.to_numpy()
    )
    result = pa.table(
        {
            "spatial_id": pa.array(cells, type=pa.int64()),
            "code": pa.array(classify(mean, *centroids), type=pa.string()),
            "diversity": pa.array(diversity, type=pa.float64()),
            "n_oa": pa.array(n_oa, type=pa.int32()),
        }
    )
    con = duckdb_connector(writeable=True)
    try:
        con.register("result", result)
        write_geoparquet(con, "SELECT * FROM result", ctx.parquet(OAC))
    finally:
        con.close()
    print(f"  {OAC}: {result.num_rows:,} rows")


DATASETS: tuple[Dataset, ...] = (
    Dataset(
        name=OVERLAPS,
        table=OVERLAPS,
        extract=extract_overlaps,
        description="Each BEAHIV cell's overlap with each 2021 output area: spatial_id, oa21cd, overlap_area (m²).",
        geometry=False,
        depends_on=("output_areas_2021",),
    ),
    Dataset(
        name=OAC,
        table=OAC,
        extract=extract_oac,
        description="OAC subgroup code per BEAHIV cell (its OAs' area-weighted mean vector, classified top-down), "
        "with diversity (weighted mean pairwise distance between its OA vectors) and n_oa.",
        geometry=False,
        depends_on=(OVERLAPS, "oac"),
    ),
)
