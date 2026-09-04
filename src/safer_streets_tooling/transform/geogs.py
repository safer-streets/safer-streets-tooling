"""``{unit}_geogs`` — one row per cell: ONS codes + overlap id lists + nearest retail centre.

Scope: a ``*_geogs`` table holds only values that are specific to the *(cell, feature)* pair — i.e.
things that genuinely vary per cell and can't be recovered from a geography code alone: the ONS codes
the cell maps to, the cell's overlap with each feature layer (``{prefix}_ids`` +
``{prefix}_overlap_area`` / ``road_overlap_length``), its ``cell_area``, and its nearest retail centre
(id + distance).

It deliberately does **not** carry attributes that are a property of a geography the cell already
references — e.g. IMD scores, which are an LSOA-level attribute. Those stay in their own table and a
consumer joins to them via the relevant code (``h3_*_geogs.lsoa21cd`` → the IMD table), so a value held
once per LSOA isn't duplicated across every cell in that LSOA.
"""

import duckdb

from safer_streets_tooling.transform.base import SpatialUnit, TransformStep, create_clause, h3_unit, table_exists
from safer_streets_tooling.transform.geo_lookups import GEOGRAPHY_MAPPINGS
from safer_streets_tooling.transform.overlap_lookups import OVERLAP_FEATURES
from safer_streets_tooling.transform.retail_centre_lookups import RETAIL_CENTRES_TABLE

# the geography used as the base table for the *_geogs tables (broadest coverage: incl. NI/Scotland).
# Validated at import: a key that isn't in the mapping used to fall back to the first geography
# (E&W-only PFA), silently narrowing every *_geogs table instead of failing.
_BASE_KEY = "lad24cd"

if _BASE_KEY not in GEOGRAPHY_MAPPINGS:
    raise ValueError(
        f"geogs base geography {_BASE_KEY!r} is not one of GEOGRAPHY_MAPPINGS: {', '.join(GEOGRAPHY_MAPPINGS)}"
    )


def build_unit(con: duckdb.DuckDBPyConnection, unit: SpatialUnit, replace: bool) -> None:
    """Create ``{unit.key}_geogs`` with one row per cell carrying every ONS code.

    Built by LEFT JOINing the per-geography lookup views on ``spatial_id``, starting from
    the broadest-coverage geography so cells outside England & Wales are still retained. A ``cell_area``
    column carries the cell's area in m² (``unit.area``: the H3 cell's true geodesic area from the h3
    extension, the polygon's ``ST_Area`` for the hotspot hexes) —
    the same unit as the ``{prefix}_overlap_area`` columns, so ``{prefix}_overlap_area / cell_area`` is a
    coverage fraction. For each overlap
    feature whose data is present (greenspace, urban/suburban land cover, road network), a ``{prefix}_ids``
    list of overlapping features is added along with an aggregate overlap measure: ``{prefix}_overlap_area``
    (m²) is the *largest* single overlap for the polygon layers (greenspace, and the ``urban`` /
    ``suburban`` land-cover splits) — overlapping polygons of different types would double-count if summed —
    while ``road_overlap_length`` (m) is the *total* road length within the cell. When retail centres are
    present, the nearest centre's ``retail_centre_id`` and ``retail_centre_distance`` are added.
    """
    base = _BASE_KEY
    others = [key for key in GEOGRAPHY_MAPPINGS if key != base]
    present = [f for f in OVERLAP_FEATURES if table_exists(con, f.table)]
    has_retail = table_exists(con, RETAIL_CENTRES_TABLE)

    select_cols = ", ".join([f"base.{base}", *(f"{key}.{key}" for key in others)])
    joins = "\n".join(f"LEFT JOIN {unit.key}_{key}_lookup {key} USING (spatial_id)" for key in others)

    # collect optional per-cell features: overlap lists, plus the scalar nearest-retail-centre
    ctes: list[str] = []
    extra_cols: list[str] = []
    extra_joins: list[str] = []
    for f in present:
        ctes.append(
            f"{f.cte} AS (SELECT spatial_id, LIST({f.prefix}_id) AS {f.prefix}_ids, "
            f"{f.agg_fn}({f.overlap_alias}) AS {f.prefix}_{f.overlap_alias} "
            f"FROM {unit.key}_{f.name}_lookup GROUP BY spatial_id)"
        )
        extra_cols.append(f"{f.cte}.{f.prefix}_ids")
        extra_cols.append(f"{f.cte}.{f.prefix}_{f.overlap_alias}")
        extra_joins.append(f"LEFT JOIN {f.cte} USING (spatial_id)")
    if has_retail:
        ctes.append(
            f"rc AS (SELECT spatial_id, retail_centre_id, distance AS retail_centre_distance "
            f"FROM {unit.key}_retail_centre_lookup)"
        )
        extra_cols.extend(["rc.retail_centre_id", "rc.retail_centre_distance"])
        extra_joins.append("LEFT JOIN rc USING (spatial_id)")

    with_clause = ("WITH " + ", ".join(ctes)) if ctes else ""
    extra_col_sql = "".join(f", {c}" for c in extra_cols)
    extra_join_sql = "\n".join(extra_joins)

    con.execute(f"""
        {create_clause("TABLE", f"{unit.key}_geogs", replace=replace)} AS
        {with_clause}
        SELECT base.spatial_id, {unit.area} AS cell_area, {select_cols}{extra_col_sql}
        FROM {unit.key}_{base}_lookup base
        {unit.area_join}
        {joins}
        {extra_join_sql};
    """)


def build(con: duckdb.DuckDBPyConnection, resolutions: list[int], replace: bool) -> None:
    for res in resolutions:
        build_unit(con, h3_unit(res), replace)


def outputs(con: duckdb.DuckDBPyConnection, resolutions: list[int]) -> list[str]:
    return [f"h3_{res}_geogs" for res in resolutions]


STEP = TransformStep(
    name="geogs",
    build=build,
    outputs=outputs,
    description="One row per H3 cell: ONS codes, overlap id lists + measures, cell_area, nearest retail centre.",
    depends_on=("geo_lookups", "overlap_lookups", "retail_centre_lookups"),
)
