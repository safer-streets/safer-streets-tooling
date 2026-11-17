"""BEAHIV hex grid (350m edge-to-edge, flat) over the police force areas → ``bh350eef.parquet``.

One row per (hex cell, police force area) covering England & Wales, derived from the
``police_force_areas`` boundaries rather than downloaded. ``overlap_area`` is the area (m²) of the cell
lying inside that force, so a cell straddling a boundary appears once per force it touches and its
overlap areas sum to the cell's area (less on a coast, where part of the cell is sea). An area rather
than a fraction, as for every overlap in this package: divide by the cell's area for its share.

beahiv works natively in British National Grid, so no reprojection happens here. ``spatial_id`` is the
cell id as a plain ``BIGINT``: every beahiv id is below 2**52, so every one of them fits a signed
64-bit column, and ``bh350eef_crime_counts`` is keyed the same way. The dataset is named by the
grid key (see :mod:`safer_streets_tooling.beahiv_grid`), so a change to the grid renames it too.
"""

import beahiv as bh
import numpy as np
import pyarrow as pa
import shapely
from safer_streets_core.database import duckdb_connector, read_geoparquet, write_geoparquet
from shapely.geometry.base import BaseGeometry

from safer_streets_tooling.beahiv_grid import KEY, MEASURE, ORIENTATION, SIZE
from safer_streets_tooling.extract.base import Dataset, ExtractContext

# small enough that clipping one cell against a tile is cheap, large enough that the subdivision
# itself does not dominate; see quad_tiles
_MAX_TILE_VERTICES = 5000


def quad_tiles(geom: BaseGeometry, max_vertices: int = _MAX_TILE_VERTICES) -> list[BaseGeometry]:
    """Split geom into disjoint tiles of <= max_vertices, so clipping against it stays local work."""
    if geom.is_empty:
        return []
    vertices = shapely.count_coordinates(geom)
    if vertices <= max_vertices:
        return [geom]
    minx, miny, maxx, maxy = geom.bounds
    mx, my = (minx + maxx) / 2, (miny + maxy) / 2
    quadrants = (
        shapely.box(minx, miny, mx, my),
        shapely.box(mx, miny, maxx, my),
        shapely.box(minx, my, mx, maxy),
        shapely.box(mx, my, maxx, maxy),
    )

    tiles: list[BaseGeometry] = []
    for quadrant in quadrants:
        part = shapely.intersection(geom, quadrant)
        if part.is_empty:
            continue
        # only recurse where splitting actually made the piece simpler. clipping adds vertices along
        # the cut, so a part can come back no smaller than its parent -- a box stays 5 coordinates
        # however finely it is split, as does a cluster of near-coincident vertices. Taking such a
        # part as a leaf keeps the vertex count strictly decreasing, which is what terminates this.
        if shapely.count_coordinates(part) < vertices:
            tiles.extend(quad_tiles(part, max_vertices))
        else:
            tiles.append(part)
    return tiles


def cell_overlap_areas(polygon: BaseGeometry, cell_polys: np.ndarray) -> np.ndarray:
    """Area (m²) of each cell falling inside polygon; the cell's own area for cells wholly within it.

    Two steps. Which cells straddle the boundary is one prepared pass over all of them — a second
    ``polyfill(predicate="full")`` diffed against the overlap set gives the identical answer but costs
    another full pass. How much of each straddler is inside then needs a real clip, which no predicate
    gives: clipping against a force's whole outline is O(its vertices) per cell, so we clip against the
    quadtree tiles instead. The tiles are disjoint, so the per-cell areas simply add.
    """
    shapely.prepare(polygon)
    overlap = shapely.area(cell_polys)
    edge = np.flatnonzero(~shapely.contains_properly(polygon, cell_polys))
    if edge.size:
        tiles = np.array(quad_tiles(polygon), dtype=object)
        cell_i, tile_i = shapely.STRtree(tiles).query(cell_polys[edge], predicate="intersects")
        clipped = np.zeros(edge.size)
        np.add.at(clipped, cell_i, shapely.area(shapely.intersection(cell_polys[edge][cell_i], tiles[tile_i])))
        overlap[edge] = clipped
    return overlap


def polyfill_force(polygon: BaseGeometry) -> tuple[np.ndarray, np.ndarray, list[BaseGeometry]]:
    """Cell ids covering polygon, the area (m²) of each inside it, and their hex outlines."""
    cell_ids = np.asarray(bh.polyfill(polygon, size=SIZE, measure=MEASURE, orientation=ORIENTATION), dtype=np.int64)
    if cell_ids.size == 0:
        return cell_ids, np.zeros(0), []
    cell_polys = np.asarray(bh.cell_polygons(cell_ids), dtype=object)
    return cell_ids, cell_overlap_areas(polygon, cell_polys), list(cell_polys)


def extract(ctx: ExtractContext) -> None:
    """Write the ``bh350eef`` parquet from the police force area boundaries."""
    pfas = ctx.parquet("police_force_areas")
    if not pfas.exists():
        raise FileNotFoundError(f"police_force_areas parquet not found: {pfas}")

    con = duckdb_connector(writeable=True)
    try:
        forces = con.execute(
            f"SELECT spatial_id, pfa24nm, ST_AsText(geom) AS wkt FROM ({read_geoparquet(pfas)}) ORDER BY spatial_id"
        ).fetchall()

        con.execute(f"""
            CREATE TABLE {KEY} (
                spatial_id BIGINT, overlap_area DOUBLE, pfa24cd VARCHAR, pfa24nm VARCHAR, geom GEOMETRY
            );
        """)
        for pfa24cd, pfa24nm, wkt in forces:
            cell_ids, overlap_area, cell_polys = polyfill_force(shapely.from_wkt(wkt))
            print(f"  {pfa24nm}: {cell_ids.size:,} cells")
            # inserted a force at a time off an arrow table: a row-wise executemany over ~1.5m rows
            # is far slower, and cannot infer the parameter type ST_GeomFromText needs
            con.register(
                "force_cells",
                pa.table(
                    {
                        "spatial_id": pa.array(cell_ids, type=pa.int64()),
                        "overlap_area": pa.array(overlap_area, type=pa.float64()),
                        "wkt": pa.array(
                            shapely.to_wkt(np.asarray(cell_polys), rounding_precision=-1), type=pa.string()
                        ),
                    }
                ),
            )
            con.execute(
                f"""
                INSERT INTO {KEY}
                SELECT spatial_id, overlap_area, ?, ?, ST_GeomFromText(wkt) FROM force_cells;
                """,
                [pfa24cd, pfa24nm],
            )
            con.unregister("force_cells")

        row_count = con.execute(f"SELECT COUNT(*) FROM {KEY}").fetchone()[0]  # ty:ignore[not-subscriptable]
        write_geoparquet(con, f"SELECT * FROM {KEY}", ctx.parquet(KEY))
    finally:
        con.close()
    print(f"  {KEY}: {row_count:,} rows")


DATASET = Dataset(
    name=KEY,
    table=KEY,
    extract=extract,
    description=f"BEAHIV {SIZE}m edge-to-edge flat hex grid over E&W police force areas; spatial_id = cell id, overlap_area = m² of the cell inside that force.",
    depends_on=("police_force_areas",),
)
