"""Registering a vectorised Python UDF on a DuckDB connection, once.

Used by the transform's steps and by the extract layers that tag each feature with its BEAHIV
cell, so it sits above both phases rather than inside either.
"""

import threading
from collections.abc import Callable

import duckdb
from duckdb.sqltypes import DuckDBPyType

# UDFs live in the catalog, which every cursor shares, so concurrent steps race to register the same
# name. Serialise the check-then-create so only one of them wins.
_UDF_LOCK = threading.Lock()


def register_udf(
    con: duckdb.DuckDBPyConnection,
    name: str,
    fn: Callable[..., object],
    params: list[DuckDBPyType],
    return_type: DuckDBPyType,
) -> None:
    """Register a vectorised (``type="arrow"``) Python UDF on ``con``, unless the catalog already has it.

    The catalog outlives a single ``build`` and is shared by every cursor, so a rebuild — or a second
    step registering the same helper on another cursor — would otherwise fail on the name already
    existing. The catalog is the thing to test: ``remove_function`` and re-create looks like the obvious
    way to make this idempotent, but once the UDF has *executed* over real data it only deregisters the
    Python side — ``duckdb_functions()`` still lists the name and ``create_function`` then raises
    ``CatalogException``. Skipping the re-registration is safe because these UDFs are pure functions of
    module-level constants, so an existing registration is by definition the same function.
    """
    with _UDF_LOCK:
        sql = "SELECT COUNT(*) FROM duckdb_functions() WHERE function_name = ?"
        if not con.execute(sql, [name]).fetchone()[0]:  # ty:ignore[not-subscriptable]
            con.create_function(name, fn, params, return_type, type="arrow")
