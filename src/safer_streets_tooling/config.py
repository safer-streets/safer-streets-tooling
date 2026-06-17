"""The data-source catalogue (remote URLs, cached filenames, layer/sheet hints) for the extract pipeline.

Mirrors the resolution in ``safer_streets_core.utils`` but rooted at *this* package's own
``config/data_sources.json``, so the tooling pipeline reads its own catalogue rather than the core
package's copy. A file in the data directory (``data_dir()/data_sources.json``) still takes precedence,
letting URLs and filenames be corrected at runtime without a redeployment; otherwise the
version-controlled default shipped in this repo's ``config/`` is used.
"""

import json
from pathlib import Path
from typing import Any

from safer_streets_core.utils import data_dir


def data_sources_path() -> Path:
    """Location of the data_sources.json catalogue (data-dir override first, else the repo default)."""
    override = data_dir() / "data_sources.json"
    return override if override.exists() else Path(__file__).parents[2] / "config" / "data_sources.json"


def data_source(key: str) -> Any:
    """Return the catalogue entry for ``key`` (a bare URL string or an object such as
    ``{"url": ..., "zip": ..., "layer": ...}``). The file is read on every call so a corrected
    catalogue is picked up without restarting."""
    path = data_sources_path()
    with open(path) as fd:
        data_sources = json.load(fd)
    if key not in data_sources:
        raise ValueError(f"key {key} does not map to a data source. Check {path}")
    return data_sources[key]
