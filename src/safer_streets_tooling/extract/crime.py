"""police.uk street-level crime archive → ``crime_data.parquet``."""

import re
from datetime import datetime, timedelta
from pathlib import Path
from zipfile import BadZipFile, ZipFile

import requests
from safer_streets_core.database import duckdb_connector, write_geoparquet
from safer_streets_core.utils import archive_path

from safer_streets_tooling.config import data_source
from safer_streets_tooling.extract._common import download, raw_dir
from safer_streets_tooling.extract.base import Dataset, ExtractContext

# police.uk publishes a new archive on (or shortly after) the 15th of the month, each release adding
# one month and dropping the oldest of the 36 it carries.
RELEASE_DAY = 15

# A month directory at the root of the archive: "2026-07/avon-and-somerset-street.csv".
MONTH = re.compile(r"\d{4}-\d{2}")


def _release_due(mtime: float, now: datetime | None = None) -> bool:
    """Has a release date — the 15th — fallen between ``mtime`` and now?

    The gate on the currency check below, not the verdict: an archive downloaded since the most
    recent 15th cannot have been superseded, so the usual run asks the network nothing at all. The
    date can only say a release is *due*; releases slip, so whether one actually happened is a
    question for the server.
    """
    now = now or datetime.now()
    latest = now.replace(day=RELEASE_DAY, hour=0, minute=0, second=0, microsecond=0)
    if now.day < RELEASE_DAY:  # this month's release is still ahead; the last one was last month's
        latest = (latest.replace(day=1) - timedelta(days=1)).replace(day=RELEASE_DAY)
    return datetime.fromtimestamp(mtime) < latest


def _published_month(url: str) -> str | None:
    """The month the police.uk ``latest.zip`` currently stands for, as ``YYYY-MM``.

    ``latest.zip`` is a 302 to the month-named archive it aliases
    (``…/archive/2026-07.zip``), so the published release can be identified from the redirect
    target alone — a HEAD that is deliberately *not* followed, costing one request rather than the
    1.7GB body. ``None`` when the redirect can't be read or understood, which is treated as "no
    evidence of a new release" rather than a reason to re-download.
    """
    try:
        response = requests.head(url, allow_redirects=False, timeout=30)
        location = response.headers.get("location", "")
    except requests.RequestException as e:
        print(f"  Could not check for a newer archive ({e})")
        return None
    match = re.search(rf"({MONTH.pattern})\.zip", location)
    if not match:
        print(f"  Could not read a release month from {location or 'the archive URL'}")
        return None
    return match.group(1)


def _cached_month(archive: Path) -> str | None:
    """The newest month the downloaded ``archive`` holds data for, as ``YYYY-MM``.

    Read from the zip's month directories, which is what the extract actually consumes — so the
    comparison is against the data on disk rather than a filename or a timestamp, and an archive
    downloaded before this check existed is classified correctly. Only the central directory is
    read, so it costs milliseconds even on a 1.7GB file. ``None`` if the file can't be opened as a
    zip, which makes it stale by definition: it is unusable either way.
    """
    try:
        with ZipFile(archive) as z:
            months = {name.split("/")[0] for name in z.namelist()}
    except (OSError, BadZipFile) as e:
        print(f"  Could not read {archive} ({e})")
        return None
    return max((m for m in months if MONTH.fullmatch(m)), default=None)


def _is_stale(url: str, archive: Path) -> bool:
    """Has police.uk published an archive newer than the cached one?"""
    if not _release_due(archive.stat().st_mtime):
        return False
    cached = _cached_month(archive)
    if cached is None:
        return True
    published = _published_month(url)
    if published is None:  # already said why; no evidence of a new release, so keep what we have
        return False
    if published <= cached:
        print(f"  Cached archive holds data to {cached}, still the published release")
        return False
    print(f"  Cached archive holds data to {cached}; {published} has been published")
    return True


def extract(ctx: ExtractContext) -> None:
    """
    Write the ``crime_data`` parquet from the police.uk bulk crime archive.

    The latest archive is downloaded (cached under the data directory, and reused only while it is
    still the published release — see :func:`_is_stale` — or unconditionally under force_download)
    and every month/force ``*-street.csv`` is read via DuckDB's zipfs. Geometry is added by
    transforming the WGS-84 longitude/latitude to BNG (EPSG:27700); the lon/lat columns are retained
    because the H3 transforms index crimes straight from them.
    """
    url = data_source("crime")["url"]
    archive = raw_dir() / archive_path("latest").name
    if ctx.force_download or not archive.exists() or _is_stale(url, archive):
        download(url, archive)
    else:
        print(f"  Using cached {archive}")

    con = duckdb_connector(writeable=True)
    try:
        con.execute("INSTALL zipfs FROM community;LOAD zipfs;")
        # limited support for **/ glob, but ????-?? is a reasonable workaround
        con.execute(f"""
            CREATE TABLE crime_data AS
            SELECT * FROM read_csv('zip://{archive}/????-??/*-street.csv', normalize_names = true);
            ALTER TABLE crime_data ADD COLUMN geom GEOMETRY;
            UPDATE crime_data
            SET geom = ST_Transform(
                    ST_Point(longitude, latitude),
                    'EPSG:4326',
                    'EPSG:27700',
                    always_xy := true
                );
        """)
        row_count = con.execute("SELECT COUNT(*) FROM crime_data").fetchone()[0]  # ty:ignore[not-subscriptable]
        write_geoparquet(con, "SELECT * FROM crime_data", ctx.parquet("crime_data"))
    finally:
        con.close()
    print(f"  crime_data: {row_count:,} rows")


DATASET = Dataset(
    name="crime_data",
    table="crime_data",
    extract=extract,
    description="police.uk street-level crimes (date, type, lat/lon, reporting force).",
    optional=False,
)
