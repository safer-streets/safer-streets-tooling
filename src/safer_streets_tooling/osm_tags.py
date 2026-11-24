"""Categorising OpenStreetMap features by their tags, shared by the ``poi`` extract and the ``beahiv_osm_*`` transforms.

A category is a set of tags; a feature takes the first category it has any tag of. Both phases need the rule, and
need it to agree (``shop=vacant`` is no more a shop on a polygon than on a point), so it lives here, above both, as
``grids`` does. Only the SQL for a tag's value differs: a lookup in QuackOSM's ``tags`` MAP for ``poi``, a plain column
on the ``osm_landuse`` table.
"""

from collections.abc import Callable, Iterable

# Each key maps to the values that count, or None for any value but NOT_A_VALUE
Tags = dict[str, list[str] | None]

# shop=vacant (about 10k nodes), shop=no, ...: not a shop
NOT_A_VALUE = ["no", "vacant", "disused"]

# shop=yes, office=yes, building=yes: the tag applies but says nothing of what kind, so it gives no subcategory
NO_DETAIL = "yes"


def _sql_list(values: list[str]) -> str:
    return ", ".join(f"'{v}'" for v in values)


def _has(column: Callable[[str], str], key: str, values: list[str] | None) -> str:
    if values is None:
        return f"coalesce({column(key)}, 'no') NOT IN ({_sql_list(NOT_A_VALUE)})"
    return f"coalesce({column(key)} IN ({_sql_list(values)}), false)"


def _in_category(column: Callable[[str], str], tags: Tags) -> str:
    return " OR ".join(_has(column, key, values) for key, values in tags.items())


def category_case(categories: dict[str, Tags], column: Callable[[str], str]) -> str:
    """A SQL ``CASE`` giving a feature the first of ``categories`` it has a tag of, or NULL if none.

    ``column(key)`` is the SQL for the feature's value of tag ``key`` (NULL when absent). The order of ``categories``
    settles a feature with tags in several.
    """
    whens = " ".join(f"WHEN {_in_category(column, tags)} THEN '{category}'" for category, tags in categories.items())
    return f"CASE {whens} END"


def subcategory_case(categories: dict[str, Tags], column: Callable[[str], str], otherwise: str = "NULL") -> str:
    """A SQL ``CASE`` giving what kind of its :func:`category_case` category a feature is: the value of one of its tags.

    The ``WHEN`` s are :func:`category_case`'s, so the subcategory always belongs to the feature's category, by
    construction rather than by keeping two rule sets in step. Each ``THEN`` takes the first of that category's tags
    the feature has with a value other than NO_DETAIL, so list the specific use before the zone (``amenity`` before
    ``landuse``). A ``yes`` still decides the category but gives way to the next tag: ``shop=yes`` +
    ``building=retail`` is a shop, subcategory ``retail``; ``shop=yes`` alone is a shop with none. A tag of another
    category (``shop=yes`` + ``landuse=commercial``) is never used. ``otherwise`` is the SQL for a feature in no
    category.
    """

    def detail(key: str, values: list[str] | None) -> str:
        return f"CASE WHEN {_has(column, key, values)} AND {column(key)} <> '{NO_DETAIL}' THEN {column(key)} END"

    whens = " ".join(
        f"WHEN {_in_category(column, tags)} THEN coalesce({', '.join(detail(k, v) for k, v in tags.items())})"
        for tags in categories.values()
    )
    return f"CASE {whens} ELSE {otherwise} END"


def first_value(keys: Iterable[str], column: Callable[[str], str]) -> str:
    """SQL for the first of ``keys`` the feature has a value of, skipping NOT_A_VALUE and NO_DETAIL; NULL if none."""
    skip = _sql_list([*NOT_A_VALUE, NO_DETAIL])
    values = (f"CASE WHEN {column(key)} NOT IN ({skip}) THEN {column(key)} END" for key in keys)
    return f"coalesce({', '.join(values)})"
