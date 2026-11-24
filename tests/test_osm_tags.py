"""Tests for the shared OSM tag rules: category, the subcategory that explains it, and the first-value fallback.

Pure SQL over literal rows in a plain in-memory DuckDB: no extensions, offline-safe.
"""

import duckdb
import pytest

from safer_streets_tooling.osm_tags import Tags, category_case, first_value, subcategory_case

KEYS = ("amenity", "shop", "landuse")

CATEGORIES: dict[str, Tags] = {
    "hospitality": {"amenity": ["pub", "cafe"]},
    "shop": {"shop": None, "amenity": ["marketplace"]},
    "education": {"amenity": ["school"], "landuse": ["education"]},
    "retail": {"landuse": ["retail"]},
}


def _column(key: str) -> str:
    return key


def _eval(expr: str, tags: dict[str, str]) -> object:
    cols = ", ".join(f"?::VARCHAR AS {key}" for key in KEYS)
    (row,) = duckdb.connect().execute(f"SELECT {expr} FROM (SELECT {cols})", [tags.get(k) for k in KEYS]).fetchall()
    return row[0]


@pytest.mark.parametrize(
    ("tags", "category", "subcategory"),
    [
        ({"amenity": "pub", "landuse": "retail"}, "hospitality", "pub"),  # the first category wins, and explains it
        ({"shop": "bakery", "landuse": "retail"}, "shop", "bakery"),  # any value counts...
        ({"shop": "vacant", "landuse": "retail"}, "retail", "retail"),  # ...but NOT_A_VALUE, which falls through
        ({"shop": "yes", "amenity": "marketplace"}, "shop", "marketplace"),  # yes decides, the next tag says what kind
        ({"shop": "yes", "landuse": "retail"}, "shop", None),  # but never a tag of another category
        ({"amenity": "school", "landuse": "education"}, "education", "school"),  # within a category, the first key
        ({"landuse": "education"}, "education", "education"),
        ({"amenity": "bench", "landuse": "retail"}, "retail", "retail"),  # an amenity in no category is no subcategory
        ({"amenity": "bench"}, None, None),  # NULL exactly when the category is
    ],
)
def test_subcategory_is_the_value_of_the_tag_that_decided_the_category(tags, category, subcategory):
    assert _eval(category_case(CATEGORIES, _column), tags) == category
    assert _eval(subcategory_case(CATEGORIES, _column), tags) == subcategory


def test_otherwise_is_only_for_a_feature_in_no_category():
    expr = subcategory_case(CATEGORIES, _column, otherwise="'fallback'")
    assert _eval(expr, {"amenity": "bench"}) == "fallback"
    assert _eval(expr, {"shop": "yes"}) is None  # in a category, with no detail: not the fallback


@pytest.mark.parametrize(
    ("tags", "value"),
    [
        ({"amenity": "bench", "shop": "bakery"}, "bench"),  # in the order of the keys
        ({"amenity": "no", "shop": "bakery"}, "bakery"),  # NOT_A_VALUE skipped
        ({"amenity": "yes", "shop": "bakery"}, "bakery"),  # and NO_DETAIL
        ({"shop": "disused"}, None),
        ({}, None),
    ],
)
def test_first_value(tags, value):
    assert _eval(first_value(KEYS, _column), tags) == value
