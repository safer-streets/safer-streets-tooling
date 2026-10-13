"""ORR estimates of station usage (annual passenger entries/exits per GB rail station) → ``station_usage.parquet``.

The Office of Rail and Road's Table 1410 gives, for every mainline station in Great Britain, one financial
year's estimated entries and exits (split by full-price / reduced / season tickets), interchanges and the
busiest origin/destination station. It carries no coordinates, so each station is located by its Three
Letter Code (the CRS code) against the NaPTAN national rail area (ATCO area 910), whose XML — unlike the
national CSV the ``naptan`` dataset reads — cross-references each rail StopPoint's CRS and TIPLOC.
"""

import csv
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import pyarrow as pa
from safer_streets_core.database import duckdb_connector, write_geoparquet

from safer_streets_tooling.config import data_source
from safer_streets_tooling.extract._common import cell_id_columns, download, raw_dir
from safer_streets_tooling.extract.base import Dataset, ExtractContext

# ORR header (whitespace collapsed) -> output column. Matched by name rather than position because the
# column order has changed between releases.
COLUMNS = {
    "Station name": "station_name",
    "Three Letter Code (TLC)": "crs",
    "National Location Code (NLC)": "nlc",
    "Region": "region",
    "Station facility owner": "facility_owner",
    "Entries and exits: Full price tickets": "entries_exits_full",
    "Entries and exits: Reduced price tickets": "entries_exits_reduced",
    "Entries and exits: Season tickets": "entries_exits_season",
    "Entries and exits: All tickets": "entries_exits",
    "Entries and exits: Rank": "entries_exits_rank",
    "Interchanges": "interchanges",
    "Main origin or destination station": "main_od_station",
    "Number of journeys to or from main origin or destination station": "main_od_journeys",
    "Quality limitations": "quality_limitations",
    "Additional information": "additional_information",
}
COUNT_COLUMNS = (
    "entries_exits_full",
    "entries_exits_reduced",
    "entries_exits_season",
    "entries_exits",
    "entries_exits_rank",
    "interchanges",
    "main_od_journeys",
)

_NAPTAN_NS = "{http://www.naptan.org.uk/}"
_PERIOD = re.compile(r"April (\d{4}) to March (\d{4})")
_NOTE = re.compile(r"\s*\[note \d+\]")
_NOT_APPLICABLE = "[z]"


def _cell(value: str) -> str | None:
    value = _NOTE.sub("", value).strip()
    return None if value == _NOT_APPLICABLE else value


def _cached(url: str, filename: str, *, force_download: bool) -> Path:
    path = raw_dir() / filename
    if path.exists() and not force_download:
        print(f"  Using cached {path}")
        return path
    download(url, path)
    return path


def read_orr_table(path: Path) -> tuple[str, pa.Table]:
    """Return the financial year the table covers (e.g. ``"2024-25"``) and its rows as a string table.

    The CSV opens with title/notes lines, then a header whose cells contain line breaks, which is why it is
    read with the csv module (which honours quoted newlines) rather than ``read_csv``. The year is taken
    from the title so it can't drift from the data when the catalogue URL is bumped to a new release.
    ORR's ``[z]`` (not applicable) becomes NULL and ``[note N]`` footnote markers are dropped.
    """
    with open(path, newline="", encoding="utf-8-sig") as fd:
        rows = list(csv.reader(fd))
    period = next((m for row in rows[:5] if row and (m := _PERIOD.search(row[0]))), None)
    if period is None:
        raise ValueError(f"{path}: no 'April YYYY to March YYYY' in the title; is this ORR Table 1410?")
    header_at = next((i for i, row in enumerate(rows) if row and row[0].strip() == "Station name"), None)
    if header_at is None:
        raise ValueError(f"{path}: no 'Station name' header row")
    header = [" ".join(h.split()) for h in rows[header_at]]
    missing = COLUMNS.keys() - set(header)
    if missing:
        raise ValueError(f"{path}: ORR columns not found: {sorted(missing)}")
    body = [row for row in rows[header_at + 1 :] if row and row[0].strip()]
    index = {name: header.index(heading) for heading, name in COLUMNS.items()}
    table = pa.table({name: pa.array([_cell(row[i]) for row in body], pa.string()) for name, i in index.items()})
    return f"{period[1]}-{period[2][2:]}", table


def read_naptan_rail(path: Path) -> pa.Table:
    """The rail StopPoints in a NaPTAN XML export: ATCO code, status, CRS, TIPLOC, station name and BNG position.

    The position is the StopPoint's own ``Place/Location``, not the ``AnnotatedRailRef/Location``, because
    the latter is absent for some stations (e.g. the Elizabeth line's Bond Street and Canary Wharf).
    """
    n = _NAPTAN_NS
    records: dict[str, list] = {k: [] for k in ("atco_code", "status", "crs", "tiploc", "name", "easting", "northing")}
    for _, sp in ET.iterparse(path):
        if sp.tag != f"{n}StopPoint":
            continue
        rail = sp.find(f".//{n}AnnotatedRailRef")
        # NaPTAN allows the grid reference either directly in <Location> or under its <Translation>
        place = f"{n}Place/{n}Location/"
        easting = sp.findtext(f"{place}{n}Easting") or sp.findtext(f"{place}{n}Translation/{n}Easting")
        northing = sp.findtext(f"{place}{n}Northing") or sp.findtext(f"{place}{n}Translation/{n}Northing")
        if rail is not None and easting and northing:
            records["atco_code"].append(sp.findtext(f"{n}AtcoCode"))
            records["status"].append(sp.get("Status", "active"))
            records["crs"].append(rail.findtext(f"{n}CrsRef"))
            records["tiploc"].append(rail.findtext(f"{n}TiplocRef"))
            records["name"].append(rail.findtext(f"{n}StationName"))
            records["easting"].append(float(easting))
            records["northing"].append(float(northing))
        sp.clear()
    return pa.table(records)


def extract(ctx: ExtractContext) -> None:
    """
    Write the ``station_usage`` parquet: ORR Table 1410 counts per station, located via NaPTAN.

    Each ORR station takes the NaPTAN rail StopPoints with its CRS code, or with the TIPLOC that
    ``crs_overrides`` maps it to, as candidate locations. Where a CRS has several (mostly duplicate points
    metres apart, but NaPTAN also files Newbury Park underground under Newbury Racecourse's NRC), the
    active one whose name best matches the ORR station name wins, then the lowest ATCO code. Stations with
    no candidate are reported and dropped. Counts are BIGINT, NULL where ORR prints ``[z]`` (e.g. a station
    closed all year, explained in ``additional_information``); ``geom`` is
    the BNG point, with ``h3r9_id`` / ``beahiv202_id`` cell ids. Both sources are downloaded automatically
    (cached unless force_download).
    """
    src = data_source("station_usage")
    period, orr = read_orr_table(_cached(src["url"], src["csv"], force_download=ctx.force_download))
    naptan = read_naptan_rail(
        _cached(src["naptan_rail_url"], src["naptan_rail_xml"], force_download=ctx.force_download)
    )
    overrides = pa.table(
        {"crs": list(src.get("crs_overrides", {})), "tiploc": list(src.get("crs_overrides", {}).values())},
        schema=pa.schema([("crs", pa.string()), ("tiploc", pa.string())]),
    )
    counts = ",\n".join(f"TRY_CAST(replace(o.{c}, ',', '') AS BIGINT) AS {c}" for c in COUNT_COLUMNS)

    con = duckdb_connector(writeable=True)
    try:
        con.register("_orr", orr)
        con.register("_naptan", naptan)
        con.register("_overrides", overrides)
        con.execute(f"""
            CREATE TABLE _located AS
            SELECT
                o.station_name,
                o.crs,
                o.nlc,
                o.region,
                o.facility_owner,
                '{period}' AS period,
                {counts},
                o.main_od_station,
                o.quality_limitations,
                o.additional_information,
                n.atco_code AS naptan_atco_code,
                ST_Point(n.easting, n.northing) AS geom
            FROM _orr o
            LEFT JOIN _overrides x USING (crs)
            JOIN _naptan n ON (CASE WHEN x.tiploc IS NULL THEN n.crs = o.crs ELSE n.tiploc = x.tiploc END)
            QUALIFY row_number() OVER (
                PARTITION BY o.crs
                ORDER BY
                    n.status = 'active' DESC,
                    jaro_winkler_similarity(
                        lower(o.station_name), lower(regexp_replace(n.name, ' (Rail )?Station$', ''))
                    ) DESC,
                    n.atco_code
            ) = 1;
        """)
        unmatched = con.execute(
            "SELECT station_name || ' (' || crs || ')' FROM _orr ANTI JOIN _located USING (crs) ORDER BY 1"
        ).fetchall()
        if unmatched:
            print(f"  WARNING: {len(unmatched)} station(s) not in NaPTAN, dropped (add to crs_overrides):")
            for (station,) in unmatched:
                print(f"    {station}")
        con.execute(f"""
            CREATE TABLE station_usage AS
            SELECT * EXCLUDE pt, {cell_id_columns(con, "ST_X(pt)", "ST_Y(pt)", "geom")}
            FROM (
                SELECT *, ST_Transform(geom, 'EPSG:27700', 'EPSG:4326', always_xy := true) AS pt
                FROM _located
            );
        """)
        row_count = con.execute("SELECT COUNT(*) FROM station_usage").fetchone()[0]  # ty:ignore[not-subscriptable]
        write_geoparquet(con, "SELECT * FROM station_usage", ctx.parquet("station_usage"))
    finally:
        con.close()
    print(f"  station_usage: {row_count:,} stations ({period})")


DATASET = Dataset(
    name="station_usage",
    table="station_usage",
    extract=extract,
    optional=False,  # naptan drops rail stop types, so skipping this would silently remove rail stations entirely
    description="ORR estimated annual passenger entries/exits and interchanges per GB rail station, located via NaPTAN.",
)
