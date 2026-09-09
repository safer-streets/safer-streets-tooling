"""``crime_counts_{key}`` — crimes counted per ONS geography code / crime type / month.

The same schema as the per-cell counts (``spatial_id`` / ``crime_type`` / ``month`` / ``count``) and the
same exclusions, on the ONS geographies rather than a grid. Split out of :mod:`.crime_counts` because it
is not H3 work and was gating every step of the H3 chain behind it.

**One spatial join per location, at the finest layer.** police.uk snaps crimes to a fixed set of
anonymised points, so 17.5M crimes sit on ~744k distinct coordinates (:mod:`.crime_locations`); each is
placed once in the 188,880 output areas — the cheapest layer there is, small polygons being what lets an
RTree prune — and every coarser code is then read off :mod:`.ons_hierarchy`. Nothing is joined against
the 44 police-force polygons, measured at 593 us per point against 2.0 us for the output areas: placing
17.5M crimes in a force directly costs ~173 minutes, and this costs nothing on top of a join that has to
happen anyway.

The exception is a location the OA layer does not cover: ~41k of the 744k sit offshore of the
statistical layers (which stop at the coastline) while still falling inside a local authority district,
whose polygons run to the full extent of the realm. Funnelling everything through the OA would drop
those crimes from the LAD and PFA counts, so they are placed directly at the LAD level — a fallback over
~41k points, not 744k, and their force follows from that LAD.
"""

import duckdb

from safer_streets_tooling.transform import crime_locations, ons_hierarchy
from safer_streets_tooling.transform.base import Grid, TransformStep, create_clause
from safer_streets_tooling.transform.crime_counts import expected_crimes
from safer_streets_tooling.transform.crime_locations import CRIME_FILTER

# one row per distinct snapped location carrying its code at every level: an in-memory intermediate,
# and the only place the crime rows are joined to geometry-derived data
LOCATION_GEOGRAPHIES = "crime_location_geographies"

# the level the fallback places at, for locations the base layer does not cover, and the level its
# code is read on from there
_FALLBACK_CODE = "lad24cd"


def _place_locations(con: duckdb.DuckDBPyConnection) -> None:
    """Build :data:`LOCATION_GEOGRAPHIES` — every distinct crime location with all five ONS codes.

    Raises if a location lands in more than one output area, which is what overlapping polygons in the
    base layer would look like and would inflate every count built from here.
    """
    ons_hierarchy.ensure(con)
    crime_locations.ensure(con)
    base = ons_hierarchy.BASE_CODE
    keys = ", ".join(crime_locations.JOIN_KEYS)

    con.execute(f"""
        CREATE OR REPLACE TABLE _placed_base AS
        {crime_locations.placed_in(con, ons_hierarchy.BASE_TABLE, base)}
    """)
    unplaced = f"(SELECT * FROM {crime_locations.TABLE} ANTI JOIN _placed_base USING ({keys}))"
    con.execute(f"""
        CREATE OR REPLACE TABLE _placed_fallback AS
        {crime_locations.placed_in(con, ons_hierarchy.GEOGRAPHY_MAPPINGS[_FALLBACK_CODE], _FALLBACK_CODE, unplaced)}
    """)

    # each code comes from the hierarchy (`h`) via the location's output area. The fallback level and
    # everything above it fall back to the direct placement (`f`) and what it resolves to (`fh`) for
    # locations the base layer doesn't cover; the levels below it have no fallback — those layers stop
    # at the same coastline the base layer does.
    from_fallback = (_FALLBACK_CODE, *ons_hierarchy.above(_FALLBACK_CODE))
    selects = []
    for code in ons_hierarchy.codes():
        if code == base:
            selects.append(f"b.{base}")
        elif code == _FALLBACK_CODE:
            selects.append(f"COALESCE(h.{code}, f.{code}) AS {code}")
        elif code in from_fallback:
            selects.append(f"COALESCE(h.{code}, fh.{code}) AS {code}")
        else:
            selects.append(f"h.{code}")

    con.execute(f"""
        CREATE OR REPLACE TABLE {LOCATION_GEOGRAPHIES} AS
        SELECT {", ".join(f"l.{k}" for k in crime_locations.JOIN_KEYS)}, {", ".join(selects)}
        FROM {crime_locations.TABLE} l
        LEFT JOIN _placed_base b USING ({keys})
        LEFT JOIN {ons_hierarchy.TABLE} h ON h.{base} = b.{base}
        LEFT JOIN _placed_fallback f USING ({keys})
        LEFT JOIN (SELECT DISTINCT {_FALLBACK_CODE}, {", ".join(ons_hierarchy.above(_FALLBACK_CODE))}
                   FROM {ons_hierarchy.TABLE} WHERE {_FALLBACK_CODE} IS NOT NULL) fh
               ON fh.{_FALLBACK_CODE} = f.{_FALLBACK_CODE};
    """)

    rows, locations = con.execute(f"""
        SELECT (SELECT COUNT(*) FROM {LOCATION_GEOGRAPHIES}), (SELECT COUNT(*) FROM {crime_locations.TABLE})
    """).fetchone()  # ty:ignore[not-iterable]
    if rows != locations:
        raise ValueError(
            f"{LOCATION_GEOGRAPHIES}: {rows:,} rows for {locations:,} locations — a location was placed "
            f"in more than one output area (overlapping polygons in {ons_hierarchy.BASE_TABLE})"
        )


def _count_per(con: duckdb.DuckDBPyConnection, code: str, expected: int, replace: bool) -> None:
    """Build ``crime_counts_{code}`` by grouping the crime rows on their location's ``code``.

    ``CRIME_FILTER`` is applied here as well as when the locations were built: BTP crimes sit on the same
    snapped points as everything else, so the join alone would let them back in. Only an upper bound can
    be asserted — a crime whose location no polygon covers has no code and simply does not appear.
    """
    con.execute(f"""
        {create_clause("TABLE", f"crime_counts_{code}", replace=replace)} AS
        SELECT g.{code} AS spatial_id, c.crime_type, c._month AS month, COUNT(*) AS count
        FROM crime_data c
        JOIN {LOCATION_GEOGRAPHIES} g USING ({", ".join(crime_locations.JOIN_KEYS)})
        WHERE {CRIME_FILTER} AND g.{code} IS NOT NULL
        GROUP BY g.{code}, c.crime_type, c._month;
    """)
    actual = con.execute(f"SELECT COALESCE(SUM(count), 0) FROM crime_counts_{code}").fetchone()[0]  # ty:ignore[not-subscriptable]
    if actual > expected:
        raise ValueError(
            f"crime_counts_{code}: counted {actual:,} crimes but only {expected:,} input rows passed the "
            f"filter — some crimes were counted in more than one area"
        )
    print(f"  crime_counts_{code}: {actual:,}/{expected:,} crimes fall within a boundary")


def build(con: duckdb.DuckDBPyConnection, replace: bool) -> None:
    """Create ``crime_counts_{key}`` for every ONS geography."""
    _place_locations(con)
    expected = expected_crimes(con)
    for code in ons_hierarchy.GEOGRAPHY_MAPPINGS:
        _count_per(con, code, expected, replace)


def outputs(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [f"crime_counts_{code}" for code in ons_hierarchy.GEOGRAPHY_MAPPINGS]


STEP = TransformStep(
    name="geography_counts",
    build=build,
    outputs=outputs,
    grid=Grid.ONS,
    description="Crimes counted per ONS geography code (PFA / LAD / MSOA / LSOA / OA) / crime_type / month (BTP excluded).",
    extract_inputs=("crime_data", *ons_hierarchy.required_tables()),
)
