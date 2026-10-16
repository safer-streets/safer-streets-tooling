"""Tests for the ORR station-usage extract: parsing Table 1410 and locating stations via NaPTAN's rail XML."""

import duckdb
import pandas as pd
import pytest
import shapely

from safer_streets_tooling.extract import station_usage
from safer_streets_tooling.extract.base import ExtractContext

# Shaped like the real Table 1410 CSV: title/notes lines, a header with embedded line breaks, thousands
# separators, [z] for not applicable and a [note N] marker on a closed station.
_ORR_HEADER = [
    "Station name",
    '"Entries and exits: \nFull price tickets"',
    '" Entries and exits: \nReduced price tickets"',
    '"Entries and exits:\nSeason tickets"',
    '"Entries and exits:\nAll tickets"',
    '"Entries and exits:\nRank"',
    "Interchanges",
    "Main origin or destination station",
    "Number of journeys to or from main origin or destination station",
    "Quality limitations",
    "Additional information",
    "National Location Code (NLC)",
    '"Three Letter Code\n(TLC)"',
    "Region",
    "Station facility owner",
]
_ORR_ROWS = [
    'Abbey Wood,"3,782,112","6,137,708","1,953,866","11,873,686",39,"1,879,075",Tottenham Court Road,"1,861,410",'
    "[z],[z],5131,ABW,London,Elizabeth line",
    'Newbury Racecourse,"20,894","82,996","10,738","114,628","1,696",0,Reading,"32,434",[z],[z],3071,NRC,'
    "South East,Great Western Railway",
    'Whitechapel,"1","2","3","6",50,"7",Liverpool Street,"5",[z],[z],5140,ZLW,London,Elizabeth line',
    "Teesside Airport [note 1],[z],[z],[z],[z],[z],[z],[z],[z],[z],Station Closed,7521,TEA,North East,Northern Trains",
    'Nowhere Halt,"1","1","1","3","2,600",0,Somewhere,"1",[z],[z],9999,NWH,Wales,TfW Rail',
]
_ORR_CSV = "\n".join(
    [
        '"Table 1410: Passenger entries, exits and interchanges by station, Great Britain, annual data, '
        'April 2024 to March 2025",,,,,,,,,,,,,,',
        "This worksheet contains one table.,,,,,,,,,,,,,,",
        '"Some shorthand is used in this table, [z] = data not applicable",,,,,,,,,,,,,,',
        ",".join(_ORR_HEADER),
        *_ORR_ROWS,
        ",,,,,,,,,,,,,,",
    ]
)


def _stop_point(atco, crs, tiploc, name, easting, northing, *, status="active", translated=False):
    grid = f"<Easting>{easting}</Easting><Northing>{northing}</Northing>"
    location = f"<Translation>{grid}</Translation>" if translated else grid
    return f"""
    <StopPoint Status="{status}">
      <AtcoCode>{atco}</AtcoCode>
      <Place><Location>{location}</Location></Place>
      <StopClassification><StopType>RLY</StopType><OffStreet><Rail>
        <AnnotatedRailRef><TiplocRef>{tiploc}</TiplocRef><CrsRef>{crs}</CrsRef><StationName>{name}</StationName>
        </AnnotatedRailRef>
      </Rail></OffStreet></StopClassification>
    </StopPoint>"""


_NAPTAN_XML = f"""<?xml version="1.0" encoding="utf-8"?>
<NaPTAN xmlns="http://www.naptan.org.uk/"><StopPoints>
  {_stop_point("9100ABWA", "ABW", "ABWA", "Abbey Wood", 546000, 179000, status="inactive")}
  {_stop_point("9100ABWD", "ABW", "ABWD", "Abbey Wood Rail Station", 547000, 179000)}
  {_stop_point("9100ILFENBP", "NRC", "ILFENBP", "Newbury Park Station", 544906, 188310)}
  {_stop_point("9100NEWBRYR", "NRC", "NEWBRYR", "Newbury Racecourse Rail Station", 448252, 166830)}
  {_stop_point("9100WCHAPEL", "ZWL", "WCHAPEL", "Whitechapel Rail Station", 534715, 181845, translated=True)}
  {_stop_point("9100TEESAIR", "TEA", "TEESAIR", "Teesside Airport Rail Station", 436800, 514200)}
</StopPoints></NaPTAN>"""

_SOURCE = {
    "url": "https://orr.example/table-1410.csv",
    "csv": "orr.csv",
    "naptan_rail_url": "https://naptan.example/rail.xml",
    "naptan_rail_xml": "naptan_rail.xml",
    "crs_overrides": {"ZLW": "WCHAPEL"},
}


def _unreachable(*args, **kwargs):
    raise AssertionError("the network was contacted")


@pytest.fixture
def orr_csv(tmp_path):
    path = tmp_path / "orr.csv"
    path.write_text(_ORR_CSV)
    return path


def test_orr_table_parsed_by_header_name(orr_csv):
    """The year comes from the title, headers are matched with their line breaks collapsed, [z] becomes NULL
    and [note N] markers are dropped from station names."""
    period, table = station_usage.read_orr_table(orr_csv)

    assert period == "2024-25"
    rows = {r["crs"]: r for r in table.to_pylist()}
    assert len(rows) == 5  # the trailing blank row is not a station
    assert rows["ABW"]["entries_exits"] == "11,873,686"
    assert rows["ABW"]["quality_limitations"] is None
    assert rows["TEA"]["station_name"] == "Teesside Airport"
    assert rows["TEA"]["entries_exits"] is None
    assert rows["TEA"]["additional_information"] == "Station Closed"


def test_orr_table_missing_column_raises(tmp_path):
    """A release that drops or renames a column fails loudly rather than writing a shifted table."""
    path = tmp_path / "orr.csv"
    path.write_text(_ORR_CSV.replace("Interchanges", "Changes"))
    with pytest.raises(ValueError, match="Interchanges"):
        station_usage.read_orr_table(path)


def test_naptan_rail_reads_direct_and_translated_grid_refs(tmp_path):
    path = tmp_path / "naptan_rail.xml"
    path.write_text(_NAPTAN_XML)
    rows = {r["atco_code"]: r for r in station_usage.read_naptan_rail(path).to_pylist()}

    assert rows["9100NEWBRYR"] == {
        "atco_code": "9100NEWBRYR",
        "status": "active",
        "crs": "NRC",
        "tiploc": "NEWBRYR",
        "name": "Newbury Racecourse Rail Station",
        "easting": 448252.0,
        "northing": 166830.0,
    }
    assert (rows["9100WCHAPEL"]["easting"], rows["9100WCHAPEL"]["northing"]) == (534715.0, 181845.0)


def test_extract_locates_stations_in_bng(tmp_path, monkeypatch):
    """Each station gets one NaPTAN location: active over inactive, best name match over a wrongly shared CRS,
    an override's TIPLOC over its CRS. Unlocated stations are dropped; a cached rerun stays offline."""
    payloads = {_SOURCE["url"]: _ORR_CSV, _SOURCE["naptan_rail_url"]: _NAPTAN_XML}

    def fake_download(url, dest):
        dest.write_text(payloads[url])

    monkeypatch.setattr(station_usage, "data_source", lambda key: _SOURCE)
    monkeypatch.setattr(station_usage, "raw_dir", lambda: tmp_path)
    monkeypatch.setattr(station_usage, "download", fake_download)
    ctx = ExtractContext(staging=tmp_path)

    try:
        station_usage.extract(ctx)
    except duckdb.HTTPException as e:  # extension download unavailable
        pytest.skip(f"extension download unavailable: {e}")

    df = pd.read_parquet(tmp_path / "station_usage.parquet").set_index("crs")
    assert sorted(df.index) == ["ABW", "NRC", "TEA", "ZLW"]  # NWH is in no NaPTAN record
    assert dict(df["naptan_atco_code"]) == {
        "ABW": "9100ABWD",  # the inactive record sorts first but loses on status
        "NRC": "9100NEWBRYR",  # not Newbury Park, despite its lower ATCO code
        "ZLW": "9100WCHAPEL",  # via crs_overrides
        "TEA": "9100TEESAIR",
    }
    assert shapely.from_wkb(df.loc["NRC", "geom"]).coords[0] == (448252, 166830)
    assert df.loc["ABW", "entries_exits"] == 11_873_686
    assert df.loc["ABW", "period"] == "2024-25"
    assert pd.isna(df.loc["TEA", "entries_exits"])
    assert df["h3r9_id"].notna().all() and df["bh350eef_id"].notna().all()

    monkeypatch.setattr(station_usage, "download", _unreachable)
    station_usage.extract(ctx)
