"""Categorising OpenStreetMap features by their tags, shared by the ``poi`` extract and the ``beahiv_osm_landuse``
transform.

A category is a set of tags; a feature takes the first category it has any tag of. Both phases need the rule, and
need it to agree (``shop=vacant`` is no more a shop on a polygon than on a point), so it lives here, above both, as
``grids`` does. Only the SQL for a tag's value differs: a lookup in QuackOSM's ``tags`` MAP for ``poi``, a plain column
on the ``osm_landuse`` table.
"""

from collections.abc import Callable

# Each key maps to the values that count, or None for any value but NOT_A_VALUE
Tags = dict[str, list[str] | None]

# shop=vacant (about 10k nodes), shop=no, ...: not a shop
NOT_A_VALUE = ["no", "vacant", "disused"]


def _sql_list(values: list[str]) -> str:
    return ", ".join(f"'{v}'" for v in values)


def category_case(categories: dict[str, Tags], column: Callable[[str], str]) -> str:
    """A SQL ``CASE`` giving a feature the first of ``categories`` it has a tag of, or NULL if none.

    ``column(key)`` is the SQL for the feature's value of tag ``key`` (NULL when absent). The order of ``categories``
    settles a feature with tags in several.
    """

    def has(key: str, values: list[str] | None) -> str:
        if values is None:
            return f"coalesce({column(key)}, 'no') NOT IN ({_sql_list(NOT_A_VALUE)})"
        return f"coalesce({column(key)} IN ({_sql_list(values)}), false)"

    whens = " ".join(
        f"WHEN {' OR '.join(has(key, values) for key, values in tags.items())} THEN '{category}'"
        for category, tags in categories.items()
    )
    return f"CASE {whens} END"
