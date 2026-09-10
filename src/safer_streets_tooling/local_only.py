"""Which build outputs must never leave this machine.

The Home Office hotspot hexes are supplied in confidence: neither the hexes themselves nor anything
derived from them may be published to the shared Azure container. Every relation built on a spatial unit
is named ``{unit key}_{dataset}`` (see ``transform.base.relation``) — ``hotspots`` (the extract itself),
``hotspots_geogs`` / ``hotspots_lad24cd_lookup`` (the lookups keyed *on* the unit) and
``hotspots_crime_counts`` / ``hotspots_building_counts`` (the counts aggregated *onto* it) — so the key
alone identifies the whole family, and a hotspot step added later is covered the day it is written
without touching this module.

Two things consult it: ``data sync``, which neither uploads nor downloads a local-only parquet, and
``index.parquet``, whose ``local_only`` column flags the tables a consumer will not find in the
container. Both read the same predicate, so the flag cannot drift from what the sync actually does.
"""

from pathlib import PurePosixPath

from safer_streets_tooling.transform.hotspots import HOTSPOT_UNIT

# Spatial-unit keys whose entire relation family stays local.
LOCAL_ONLY_UNITS: frozenset[str] = frozenset({HOTSPOT_UNIT.key})


def is_local_only(name: str) -> bool:
    """True when ``name`` — a table name, parquet filename or blob name — belongs to a local-only unit.

    Matches the bare key and the ``{key}_`` prefix, which is how every relation is named now, plus the
    ``_{key}`` suffix the counts used before the naming convention was settled — parquet in that older
    shape are still in the container and on machines, and must stay just as excluded. Deliberately
    broad: a table that merely looks like it belongs to the family is held back rather than published.
    """
    stem = PurePosixPath(name).stem
    return any(stem == key or stem.startswith(f"{key}_") or stem.endswith(f"_{key}") for key in LOCAL_ONLY_UNITS)
