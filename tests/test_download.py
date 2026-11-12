"""Tests for ``_common.download``: a download only lands at ``dest`` once complete, and an interrupted one
resumes from its ``.part`` when the server confirms (via ``If-Range``) that the file hasn't changed."""

from collections.abc import Iterator
from pathlib import Path

import pytest
import requests

from safer_streets_tooling.extract import _common

CONTENT = bytes(range(256)) * 40  # 10 KiB
ETAG = '"v1"'


class FakeResponse:
    """Enough of ``requests.Response`` for ``download``: status, headers, a chunked body, context manager."""

    def __init__(self, status_code: int, body: bytes, headers: dict[str, str], *, cut_after: int | None = None):
        self.status_code = status_code
        self.headers = headers
        self._body = body
        self._cut_after = cut_after

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))

    def iter_content(self, chunk_size: int) -> Iterator[bytes]:
        for start in range(0, len(self._body), 1024):
            if self._cut_after is not None and start >= self._cut_after:
                raise requests.ConnectionError("connection dropped")
            yield self._body[start : start + 1024]

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *exc: object) -> None:
        pass


class FakeServer:
    """Serves ``content`` with ``etag`` and honours ``Range`` + ``If-Range`` the way an HTTP server does."""

    def __init__(self, content: bytes, etag: str | None = ETAG, *, cut_after: int | None = None):
        self.content = content
        self.etag = etag
        self.cut_after = cut_after
        self.requests: list[dict[str, str]] = []

    def get(self, url: str, headers: dict[str, str], stream: bool, timeout: int) -> FakeResponse:
        self.requests.append(headers)
        base = {"ETag": self.etag} if self.etag else {}
        if "Range" in headers and headers.get("If-Range") == self.etag:
            start = int(headers["Range"].removeprefix("bytes=").removesuffix("-"))
            if start >= len(self.content):
                return FakeResponse(416, b"", base)
            body = self.content[start:]
            return FakeResponse(206, body, base | {"content-length": str(len(body))})
        cut = self.cut_after
        self.cut_after = None  # only the first full response drops
        return FakeResponse(200, self.content, base | {"content-length": str(len(self.content))}, cut_after=cut)


@pytest.fixture
def dest(tmp_path: Path) -> Path:
    return tmp_path / "file.zip"


def _part(dest: Path) -> Path:
    return dest.with_name(dest.name + ".part")


def _validator(dest: Path) -> Path:
    return dest.with_name(dest.name + ".part.validator")


def _serve(monkeypatch: pytest.MonkeyPatch, server: FakeServer) -> FakeServer:
    monkeypatch.setattr(_common.requests, "get", server.get)
    return server


def test_complete_download_lands_at_dest_with_no_leftovers(monkeypatch, dest):
    _serve(monkeypatch, FakeServer(CONTENT))
    _common.download("https://example.org/file.zip", dest)
    assert dest.read_bytes() == CONTENT
    assert not _part(dest).exists()
    assert not _validator(dest).exists()


def test_interrupted_download_leaves_no_dest_but_keeps_the_part(monkeypatch, dest):
    _serve(monkeypatch, FakeServer(CONTENT, cut_after=4096))
    with pytest.raises(requests.ConnectionError):
        _common.download("https://example.org/file.zip", dest)
    assert not dest.exists()  # so a rerun can't mistake it for a cached copy
    assert _part(dest).read_bytes() == CONTENT[:4096]
    assert _validator(dest).read_text() == ETAG


def test_rerun_resumes_from_the_part(monkeypatch, dest):
    server = _serve(monkeypatch, FakeServer(CONTENT, cut_after=4096))
    with pytest.raises(requests.ConnectionError):
        _common.download("https://example.org/file.zip", dest)
    _common.download("https://example.org/file.zip", dest)
    assert dest.read_bytes() == CONTENT
    assert server.requests[-1]["Range"] == "bytes=4096-"
    assert server.requests[-1]["If-Range"] == ETAG


def test_changed_file_restarts_instead_of_splicing(monkeypatch, dest):
    _part(dest).write_bytes(CONTENT[:4096])
    _validator(dest).write_text('"old"')
    new = b"new version" * 100
    _serve(monkeypatch, FakeServer(new, etag='"v2"'))
    _common.download("https://example.org/file.zip", dest)
    assert dest.read_bytes() == new


def test_part_without_validator_is_not_resumed(monkeypatch, dest):
    _part(dest).write_bytes(b"junk")
    server = _serve(monkeypatch, FakeServer(CONTENT, etag=None))
    _common.download("https://example.org/file.zip", dest)
    assert dest.read_bytes() == CONTENT
    assert "Range" not in server.requests[0]


def test_part_longer_than_file_restarts(monkeypatch, dest):
    _part(dest).write_bytes(CONTENT + b"extra")
    _validator(dest).write_text(ETAG)
    _serve(monkeypatch, FakeServer(CONTENT))
    _common.download("https://example.org/file.zip", dest)
    assert dest.read_bytes() == CONTENT


def test_stream_ending_short_of_content_length_raises(monkeypatch, dest):
    def short_get(url, headers, stream, timeout):
        return FakeResponse(200, CONTENT[:100], {"content-length": str(len(CONTENT))})

    monkeypatch.setattr(_common.requests, "get", short_get)
    with pytest.raises(OSError, match="Incomplete download"):
        _common.download("https://example.org/file.zip", dest)
    assert not dest.exists()
