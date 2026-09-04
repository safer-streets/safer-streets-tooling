"""Tests for the beahiv_202 hex-grid extract.

Offline-safe: the extractor's only input is the upstream police_force_areas parquet, which these
tests synthesise, so nothing is downloaded. Skipped when DuckDB cannot fetch its spatial extension.
"""

import beahiv as bh
import duckdb
import numpy as np
import pytest
import shapely
from safer_streets_core.database import duckdb_connector, write_geoparquet

from safer_streets_tooling.extract import BY_NAME, DATASETS
from safer_streets_tooling.extract.base import ExtractContext
from safer_streets_tooling.extract.beahiv_202 import (
    ORIENTATION,
    SIDE_LENGTH,
    cell_proportions,
    extract,
    polyfill_force,
    quad_tiles,
)

HEX_AREA = 3 * np.sqrt(3) / 2 * SIDE_LENGTH**2


def _write_pfas(tmp_path, forces):
    """Write a police_force_areas parquet holding (code, name, wkt) rows."""
    try:
        con = duckdb_connector(writeable=True)
    except duckdb.HTTPException:
        pytest.skip("spatial extension unavailable")
    try:
        con.execute("CREATE TABLE p (spatial_id VARCHAR, pfa24nm VARCHAR, geom GEOMETRY);")
        con.executemany("INSERT INTO p VALUES (?, ?, ST_GeomFromText(?::VARCHAR))", forces)
        write_geoparquet(con, "SELECT * FROM p", tmp_path / "police_force_areas.parquet")
    finally:
        con.close()


def _square(x, y, size):
    return shapely.box(x, y, x + size, y + size)


def _row(con, sql):
    """First row of sql, asserted present — fetchone() is Optional, and an empty result is a test failure."""
    row = con.execute(sql).fetchone()
    assert row is not None, f"no rows: {sql}"
    return row


def test_registered_after_its_dependency():
    """The registry validator requires police_force_areas to precede beahiv_202."""
    names = [ds.name for ds in DATASETS]
    assert names.index("police_force_areas") < names.index("beahiv_202")
    assert BY_NAME["beahiv_202"].depends_on == ("police_force_areas",)


def test_quad_tiles_returns_whole_geometry_when_small():
    geom = _square(430000, 430000, 1000)
    assert quad_tiles(geom) == [geom]


def test_quad_tiles_partitions_without_gaps_or_overlaps():
    """Tiles are disjoint and their areas sum back to the original — the property proportions rely on."""
    geom = shapely.Point(430000, 430000).buffer(5000, quad_segs=64)
    tiles = quad_tiles(geom, max_vertices=20)
    assert len(tiles) > 1
    assert max(shapely.count_coordinates(t) for t in tiles) <= 20
    assert sum(t.area for t in tiles) == pytest.approx(geom.area)
    assert shapely.union_all(tiles).area == pytest.approx(geom.area)


def test_quad_tiles_terminates_when_splitting_cannot_simplify():
    """A box is 5 coordinates however finely it is cut, so recursion must stop rather than run away."""
    tiles = quad_tiles(_square(430000, 430000, 10000), max_vertices=4)
    assert len(tiles) == 4
    assert sum(t.area for t in tiles) == pytest.approx(10000**2)


def test_quad_tiles_empty_geometry():
    assert quad_tiles(shapely.from_wkt("POLYGON EMPTY")) == []


def test_interior_cells_are_whole_and_edge_cells_are_partial():
    geom = _square(430000, 430000, 5000)
    cell_ids, proportion, polys = polyfill_force(geom)

    assert cell_ids.size == len(proportion) == len(polys)
    assert proportion.min() > 0.0
    assert proportion.max() == pytest.approx(1.0)
    assert (proportion == 1.0).sum() > 0  # interior
    assert (proportion < 1.0).sum() > 0  # straddling the square's edge


def test_proportions_reproduce_the_polygon_area():
    """sum(proportion) * hex area == the polygon's own area: the grid tiles it exactly."""
    geom = _square(430000, 430000, 5000)
    _, proportion, _ = polyfill_force(geom)
    assert (proportion.sum() * HEX_AREA) == pytest.approx(geom.area, rel=1e-9)


def test_proportions_match_a_direct_clip():
    """The tiled clip agrees with intersecting each cell against the whole polygon."""
    geom = shapely.union_all([_square(430000, 430000, 3000), _square(432000, 432500, 2000)])
    cell_ids, proportion, polys = polyfill_force(geom)
    cell_polys = np.asarray(polys, dtype=object)

    direct = shapely.area(shapely.intersection(geom, cell_polys)) / shapely.area(cell_polys)
    assert np.abs(proportion - direct).max() < 1e-9
    assert cell_ids.size == len(direct)


def test_cell_proportions_all_inside():
    """A polygon far larger than the cells leaves every proportion at exactly 1.0."""
    geom = _square(400000, 400000, 50000)
    cell_ids = np.asarray(bh.polyfill(_square(430000, 430000, 2000), SIDE_LENGTH, ORIENTATION), dtype=np.uint64)
    cell_polys = np.asarray(bh.cell_polygons(cell_ids), dtype=object)
    assert (cell_proportions(geom, cell_polys) == 1.0).all()


def test_polyfill_force_empty_polygon():
    cell_ids, proportion, polys = polyfill_force(shapely.from_wkt("POLYGON EMPTY"))
    assert cell_ids.size == 0
    assert proportion.size == 0
    assert polys == []


def test_extract_writes_one_row_per_cell_and_force(tmp_path):
    """Two adjacent forces: shared boundary cells appear under both, and their proportions sum to 1."""
    _write_pfas(
        tmp_path,
        [
            ("E23000001", "Westshire", _square(430000, 430000, 2000).wkt),
            ("E23000002", "Eastshire", _square(432000, 430000, 2000).wkt),
        ],
    )
    extract(ExtractContext(staging=tmp_path))
    out = tmp_path / "beahiv_202.parquet"
    assert out.exists()

    con = duckdb_connector(writeable=True)
    try:
        con.execute(f"CREATE TABLE g AS SELECT * FROM read_parquet('{out.as_posix()}')")
        cols = [r[1] for r in con.execute("PRAGMA table_info('g')").fetchall()]
        assert cols == ["spatial_id", "proportion", "pfa24cd", "pfa24nm", "geom"]

        rows, cells, forces = _row(con, "SELECT count(*), count(DISTINCT spatial_id), count(DISTINCT pfa24cd) FROM g")
        assert forces == 2
        assert rows > cells  # the shared boundary is double-counted, one row per force

        assert _row(con, "SELECT count(*) FROM g WHERE proportion <= 0 OR proportion > 1")[0] == 0
        assert _row(con, "SELECT count(*) FROM g WHERE proportion = 1.0")[0] > 0

        # a cell touching both forces is split between them, never duplicated
        shared = _row(
            con,
            "SELECT max(total) FROM (SELECT spatial_id, sum(proportion) total FROM g GROUP BY 1 HAVING count(*) > 1)",
        )[0]
        assert shared == pytest.approx(1.0, abs=1e-9)

        # geometry survives the round trip as the cell's own hexagon
        spatial_id, wkt = _row(con, "SELECT spatial_id, ST_AsText(geom) FROM g LIMIT 1")
        assert shapely.from_wkt(wkt).equals(bh.cell_polygon(spatial_id))
    finally:
        con.close()


def test_extract_requires_its_dependency(tmp_path):
    with pytest.raises(FileNotFoundError, match="police_force_areas"):
        extract(ExtractContext(staging=tmp_path))
