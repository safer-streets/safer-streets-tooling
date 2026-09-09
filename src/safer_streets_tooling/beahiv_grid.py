"""The BEAHIV grid's parameters, shared by the extract that tiles it and the transform that counts onto it.

Both phases must agree on the side length and orientation or nothing lines up: the extract's
``beahiv_202`` cells and the transform's ``crime_counts_beahiv_202`` cells are joined on ``spatial_id``,
which *is* the encoded (q, r, side_length, orientation). Two independent copies of these constants
would produce two disjoint grids that still join without error — every row simply missing — so they
live here once and neither phase declares its own.

A 202 m side gives a cell of ~0.106 km², within a percent of an H3 resolution-9 cell, so the two
griddings are directly comparable on identical data — which is the reason this grid exists alongside
H3 rather than replacing it.
"""

import math

from beahiv import Orientation

SIDE_LENGTH = 202
ORIENTATION = Orientation.FLAT

# the infix every relation on this grid carries: the beahiv_202 extract, crime_counts_beahiv_202,
# beahiv_202_geogs. Derived from the side length so a change to the grid renames its tables rather
# than silently redefining what an existing name means.
KEY = f"beahiv_{SIDE_LENGTH}"

# a flat hexagon of side s has area 3*sqrt(3)/2 * s². It is a constant because the grid is equal-area
# in EPSG:27700 — and a *planar* BNG area, which is the right denominator for the planar
# {prefix}_overlap_area columns, unlike h3_cell_area's geodesic m².
CELL_AREA = 1.5 * math.sqrt(3.0) * SIDE_LENGTH**2
