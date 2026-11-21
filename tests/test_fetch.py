"""Tests for src/fetch.py — HTTP is faked with a tiny in-memory session (no network)."""
from __future__ import annotations

import json

import pytest

from src import fetch


# --- test doubles -----------------------------------------------------------------------------

class FakeResponse:
    def __init__(self, status_code=200, payload=None, headers=None):
        self.status_code = status_code
        self._payload = payload or {}
        self.headers = headers or {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise fetch.requests.HTTPError(f"HTTP {self.status_code}")


class FakeSession:
    """Returns queued responses in order and records every call's params."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": dict(params or {})})
        return self._responses.pop(0)


def study(nct_id):
    return {"protocolSection": {"identificationModule": {"nctId": nct_id}}}


def page(ids, next_token=None, total=None):
    payload = {"studies": [study(i) for i in ids]}
    if next_token:
        payload["nextPageToken"] = next_token
    if total is not None:
        payload["totalCount"] = total
    return FakeResponse(200, payload)


no_sleep = lambda seconds: None  # noqa: E731


# --- build_params -----------------------------------------------------------------------------

def test_build_params_adds_last_update_filter_when_since_given():
    params = fetch.build_params(updated_since="2026-11-19")
    assert params["filter.advanced"] == "AREA[LastUpdatePostDate]RANGE[2026-11-19,MAX]"


def test_build_params_requests_full_records_by_default():
    params = fetch.build_params(condition="lung cancer")
    assert params["query.cond"] == "lung cancer"
    assert "fields" not in params  # raw zone keeps the full study record


def test_build_params_uses_max_page_size_and_counts_total():
    params = fetch.build_params()
    assert params["pageSize"] == 1000
    assert params["countTotal"] == "true"


# --- paging -----------------------------------------------------------------------------------

def test_iter_pages_follows_next_page_token_and_sends_count_only_once():
    session = FakeSession([page(["NCT1", "NCT2"], next_token="tok", total=3), page(["NCT3"])])
    pages = list(fetch.iter_pages(fetch.build_params(), session=session, sleep=no_sleep))

    assert [len(p.studies) for p in pages] == [2, 1]
    assert pages[0].total_count == 3
    assert session.calls[1]["params"]["pageToken"] == "tok"
    assert "countTotal" not in session.calls[1]["params"]


def test_iter_pages_stops_at_max_records_and_shrinks_last_page_size():
    session = FakeSession([page(["NCT1", "NCT2"], next_token="tok", total=10), page(["NCT3"])])
    params = fetch.build_params(page_size=2)
    pages = list(fetch.iter_pages(params, max_records=3, session=session, sleep=no_sleep))

    assert sum(len(p.studies) for p in pages) == 3
    assert session.calls[1]["params"]["pageSize"] == 1


def test_iter_pages_pauses_between_requests_for_rate_limit():
    slept = []
    session = FakeSession([page(["NCT1"], next_token="tok", total=2), page(["NCT2"])])
    list(fetch.iter_pages(fetch.build_params(), session=session, sleep=slept.append))
    assert slept == [fetch.MIN_REQUEST_INTERVAL]


# --- retry ------------------------------------------------------------------------------------

def test_get_with_retry_honours_retry_after_header_on_429():
    slept = []
    session = FakeSession([FakeResponse(429, headers={"Retry-After": "7"}), page(["NCT1"])])
    data = fetch.get_with_retry(session, fetch.API_URL, {}, sleep=slept.append)
    assert data["studies"][0]["protocolSection"]["identificationModule"]["nctId"] == "NCT1"
    assert slept == [7.0]


def test_get_with_retry_backs_off_on_5xx_then_gives_up():
    slept = []
    session = FakeSession([FakeResponse(503)] * 3)
    with pytest.raises(fetch.requests.HTTPError):
        fetch.get_with_retry(session, fetch.API_URL, {}, attempts=3, sleep=slept.append)
    assert len(slept) == 2
    assert 1 <= slept[0] < 2 and 2 <= slept[1] < 3  # exponential base + jitter < 1s


def test_get_with_retry_does_not_retry_client_errors():
    session = FakeSession([FakeResponse(400)])
    with pytest.raises(fetch.requests.HTTPError):
        fetch.get_with_retry(session, fetch.API_URL, {}, sleep=no_sleep)
    assert len(session.calls) == 1


# --- data version -----------------------------------------------------------------------------

def test_get_data_timestamp_reads_version_endpoint():
    session = FakeSession([FakeResponse(200, {"apiVersion": "2.0.5", "dataTimestamp": "2026-11-20T10:00:05"})])
    assert fetch.get_data_timestamp(session=session) == "2026-11-20T10:00:05"
    assert session.calls[0]["url"] == fetch.VERSION_URL


# --- ingest: write pages + manifest, reconcile counts -------------------------------------------

def test_ingest_writes_one_file_per_page_and_a_manifest(tmp_path):
    session = FakeSession([page(["NCT1", "NCT2"], next_token="tok", total=3), page(["NCT3"])])
    manifest = fetch.ingest(
        out_dir=tmp_path, run_date="2026-11-21", updated_since="2026-11-19",
        data_timestamp="2026-11-20T10:00:05", session=session, sleep=no_sleep,
    )

    run_dir = tmp_path / "date=2026-11-21"
    assert sorted(p.name for p in run_dir.glob("page-*.json")) == ["page-0001.json", "page-0002.json"]
    assert json.loads((run_dir / "page-0001.json").read_text())[0]["protocolSection"]["identificationModule"]["nctId"] == "NCT1"

    saved = json.loads((run_dir / "_manifest.json").read_text())
    assert saved == manifest
    assert manifest["total_count"] == 3
    assert manifest["received"] == 3
    assert manifest["updated_since"] == "2026-11-19"
    assert manifest["data_timestamp"] == "2026-11-20T10:00:05"


def test_ingest_fails_when_received_count_does_not_match_total(tmp_path):
    session = FakeSession([page(["NCT1"], total=5)])  # API claims 5, sends 1, no next page
    with pytest.raises(fetch.CountMismatchError):
        fetch.ingest(out_dir=tmp_path, run_date="2026-11-21", session=session, sleep=no_sleep)


def test_ingest_with_max_records_expects_the_capped_count(tmp_path):
    session = FakeSession([page(["NCT1", "NCT2"], total=500)])
    manifest = fetch.ingest(out_dir=tmp_path, run_date="2026-11-21", max_records=2,
                            session=session, sleep=no_sleep)
    assert manifest["received"] == 2
    assert manifest["expected"] == 2
