"""``ons_hierarchy`` — one row per 2021 output area carrying the codes it nests inside.

The ONS geographies form a strict hierarchy: an OA nests inside one LSOA, inside one MSOA, inside one
LAD; and a police force area is a union of whole LADs (measured: of the 320 LADs that meet a PFA at all,
311 are >99.9% inside one, and the rest differ only by generalisation slivers — none straddles two
forces). So a point's LSOA / MSOA / LAD / PFA follow from its OA, and only the OA needs a spatial join.

That matters because the layers are wildly different to join against. Measured on 20k points, per point:

    output_areas_2021         188,880 polygons     2.0 us
    lsoa_2021                  35,672 polygons     2.8 us
    msoa_2021                   7,264 polygons     2.9 us
    local_authority_districts     361 polygons    10.9 us
    police_force_areas             44 polygons   592.9 us   <-- 300x the OA layer

Fewer, bigger polygons is the pathological case: 44 PFA shapes tile the country in "full extent"
geometry (~1 MB of vertices each), so an RTree prunes almost nothing and every candidate goes to an
exact containment test against a million-vertex polygon. Assigning 17.5M crimes to a PFA directly costs
~173 minutes; deriving it from the OA costs nothing on top of a join the pipeline wants anyway.

The hierarchy itself is built from representative *points* — ``ST_PointOnSurface``, which is guaranteed
to lie inside its polygon where a centroid is not — so each level is 188,880 point tests at worst. It is
a build intermediate (in memory, no parquet) and idempotent: :func:`ensure` is called by each step that
needs it, so no step has to depend on another purely to have it built.
"""

import duckdb

from safer_streets_tooling.transform.base import table_exists

TABLE = "ons_hierarchy"

# short code -> boundary table name (the tables created by ons_boundaries.load_all). The single registry
# of the ONS geographies: `geo_lookups` re-exports it as GEOGRAPHY_MAPPINGS, which is where the rest of
# the transform has always read it from. lad24 is listed for full-UK coverage; it is the base for the
# *_geogs tables.
GEOGRAPHY_MAPPINGS = {
    "pfa24cd": "police_force_areas",
    "lad24cd": "local_authority_districts",
    "msoa21cd": "msoa_2021",
    "lsoa21cd": "lsoa_2021",
    "oa21cd": "output_areas_2021",
}

# The finest level, and the only one a point is ever joined to.
BASE_CODE = "oa21cd"
BASE_TABLE = GEOGRAPHY_MAPPINGS[BASE_CODE]

# (code, boundary table, the code whose polygon contains it) — coarsest last, each resolved from the
# level before it. Order matters: a roll-up reads the level below, so they must be built in this order.
NESTING: tuple[tuple[str, str, str], ...] = tuple(
    (code, GEOGRAPHY_MAPPINGS[code], child)
    for code, child in (
        ("lsoa21cd", "oa21cd"),
        ("msoa21cd", "lsoa21cd"),
        ("lad24cd", "msoa21cd"),
        ("pfa24cd", "lad24cd"),
    )
)

if {BASE_CODE, *(code for code, _, _ in NESTING)} != set(GEOGRAPHY_MAPPINGS):
    raise ValueError("every ONS geography must appear exactly once in the hierarchy (base + NESTING)")

# Coastal layers are generalised independently, so a representative point can fall just outside the
# containing polygon. A handful of those is expected; a systematic failure (a vintage mismatch, say)
# is not, and must not pass silently as a column of NULLs.
_MAX_UNRESOLVED = 0.001


def codes() -> tuple[str, ...]:
    """Every geography code, finest first."""
    return (BASE_CODE, *(code for code, _, _ in NESTING))


def above(code: str) -> tuple[str, ...]:
    """The codes coarser than ``code`` — the levels that can be read off it by nesting."""
    ordered = codes()
    return ordered[ordered.index(code) + 1 :]


def required_tables() -> tuple[str, ...]:
    """The extract tables :func:`ensure` reads — the boundary layers, base level first."""
    return (BASE_TABLE, *(table for _, table, _ in NESTING))


def available(con: duckdb.DuckDBPyConnection) -> bool:
    return all(table_exists(con, table) for table in required_tables())


_TABLE_OF = {BASE_CODE: BASE_TABLE, **{code: table for code, table, _ in NESTING}}


def _nest(child_code: str, parent_code: str, parent_table: str) -> str:
    """SQL mapping each ``child_code`` to the ``parent_code`` whose polygon contains its inner point."""
    return f"""
        SELECT c.{child_code}, p.spatial_id AS {parent_code}
        FROM (SELECT spatial_id AS {child_code}, ST_PointOnSurface(geom) AS pt FROM {_TABLE_OF[child_code]}) c
        LEFT JOIN {parent_table} p ON ST_Contains(p.geom, c.pt)
    """


def ensure(con: duckdb.DuckDBPyConnection) -> None:
    """Create ``ons_hierarchy`` (oa21cd → lsoa21cd → msoa21cd → lad24cd → pfa24cd) unless it exists.

    Each level is a point-in-polygon of the child level's representative points against the parent
    layer — 188,880 tests at the OA level, 361 at the LAD level — and the levels are chained by LEFT
    JOIN so every OA keeps a row even where a coastal point falls outside its parent. Raises if more
    than :data:`_MAX_UNRESOLVED` of OAs fail to resolve at any level, which is what a vintage mismatch
    between the boundary layers would look like.
    """
    if table_exists(con, TABLE):
        return

    ctes, joins, cols = [], [], [f"base.{BASE_CODE}"]
    for code, table, child in NESTING:
        ctes.append(f"{code}_of AS ({_nest(child, code, table)})")
        joins.append(f"LEFT JOIN {code}_of USING ({child})")
        cols.append(f"{code}_of.{code}")

    con.execute(f"""
        CREATE TABLE {TABLE} AS
        WITH {", ".join(ctes)}
        SELECT {", ".join(cols)}
        FROM (SELECT spatial_id AS {BASE_CODE} FROM {BASE_TABLE}) base
        {chr(10).join(joins)};
    """)

    total, distinct = con.execute(  # ty:ignore[not-iterable]
        f"SELECT COUNT(*), COUNT(DISTINCT {BASE_CODE}) FROM {TABLE}"
    ).fetchone()
    if total != distinct:
        raise ValueError(
            f"{TABLE}: {total:,} rows for {distinct:,} output areas — an area resolved to more than one "
            f"parent, so the boundary layers do not nest (overlapping polygons in a coarser layer)"
        )
    for code, _, _ in NESTING:
        missing = con.execute(f"SELECT COUNT(*) FROM {TABLE} WHERE {code} IS NULL").fetchone()[0]  # ty:ignore[not-subscriptable]
        share = missing / total if total else 0.0
        if share > _MAX_UNRESOLVED:
            raise ValueError(
                f"{TABLE}: {missing:,}/{total:,} ({share:.2%}) output areas have no {code} — the boundary "
                f"layers do not nest, which usually means their vintages disagree"
            )
        if missing:
            print(f"  {TABLE}: {missing:,}/{total:,} OAs without a {code} (generalisation slivers)")
    print(f"  {TABLE}: {total:,} output areas resolved through {' -> '.join(c for c, _, _ in NESTING)}")
