"""How the grids are named, shared by both phases.

The extract phase tags each point layer with the cell it falls in on every grid, and the transform
phase aggregates onto those grids; both must spell a grid the same way or the columns and the relations
drift apart. The keys live here, above both phases, and every name is derived from them:

* a **relation** is ``{grid key}_{dataset}`` — ``h3r9_crime_counts``, ``hotspots_geogs``,
  ``beahiv202_greenspace_lookup`` (see :func:`relation`);
* a **cell-id column** on an extract layer is ``{grid key}_id`` — ``h3r9_id``, ``beahiv202_id``
  (see :data:`H3_ID` / :data:`BEAHIV_ID`).

A grid key is a single token with no internal ``_`` (``h3r9``, not ``h3_9``; ``beahiv202``, not
``beahiv_202``), so exactly one underscore separates the grid from what follows it and the key stays
greppable as one word.
"""

from safer_streets_tooling.beahiv_grid import KEY as BEAHIV_KEY

# The H3 resolutions the transform aggregates onto. The extract layers are tagged at H3_RESOLUTION,
# which must be one of them or nothing joins to the cells that were built.
H3_RESOLUTIONS = [9]
H3_RESOLUTION = 9


def h3_key(res: int) -> str:
    """The key of the H3 grid at resolution ``res`` — the prefix all its relations carry."""
    return f"h3r{res}"


def relation(grid_key: str, dataset: str) -> str:
    """The name of the relation holding ``dataset`` on the grid ``grid_key``: ``{grid}_{dataset}``.

    Mint names through here rather than by hand: the counts used to trail the grid
    (``building_counts_hotspots``) while the lookups and geogs led with it, and nothing stopped the two
    conventions drifting further apart.
    """
    return f"{grid_key}_{dataset}"


def id_column(grid_key: str) -> str:
    """The name of the column carrying a feature's cell id on the grid ``grid_key``."""
    return f"{grid_key}_id"


if H3_RESOLUTION not in H3_RESOLUTIONS:
    raise ValueError(f"H3_RESOLUTION {H3_RESOLUTION} is not one of H3_RESOLUTIONS {H3_RESOLUTIONS}")

# the cell-id columns every point extract carries, one per grid
H3_ID = id_column(h3_key(H3_RESOLUTION))
BEAHIV_ID = id_column(BEAHIV_KEY)
