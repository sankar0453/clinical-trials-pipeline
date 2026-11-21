"""Tests for src/state.py — the incremental-load watermark."""
from __future__ import annotations

from src import state


def test_read_watermark_returns_none_when_no_file(tmp_path):
    assert state.read_watermark(tmp_path / "watermark.json") is None


def test_write_then_read_round_trips(tmp_path):
    path = tmp_path / "nested" / "watermark.json"
    state.write_watermark(path, data_timestamp="2026-11-20T10:00:05")
    assert state.read_watermark(path) == {"data_timestamp": "2026-11-20T10:00:05", "loaded_through": "2026-11-20"}


def test_since_date_overlaps_by_given_days():
    wm = {"data_timestamp": "2026-11-20T10:00:05", "loaded_through": "2026-11-20"}
    assert state.since_date(wm, overlap_days=2) == "2026-11-18"


def test_is_new_data_false_when_source_timestamp_unchanged():
    wm = {"data_timestamp": "2026-11-20T10:00:05", "loaded_through": "2026-11-20"}
    assert state.is_new_data(wm, "2026-11-20T10:00:05") is False
    assert state.is_new_data(wm, "2026-11-21T10:00:03") is True
    assert state.is_new_data(None, "2026-11-21T10:00:03") is True
