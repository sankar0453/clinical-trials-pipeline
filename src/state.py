"""Incremental-load watermark: remembers how far the pipeline has loaded.

Locally this is a small JSON file (data/state/watermark.json); in AWS the same JSON lives in S3.

    {"data_timestamp": "2026-11-20T10:00:05", "loaded_through": "2026-11-20"}

- data_timestamp:  the registry's data version (from /api/v2/version) we last loaded in full.
- loaded_through:  its date part; the next run asks for trials updated since this date minus an overlap.
"""
from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path


def read_watermark(path: str | Path) -> dict | None:
    path = Path(path)
    if not path.exists():
        return None
    return json.loads(path.read_text())


def write_watermark(path: str | Path, data_timestamp: str) -> dict:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    wm = {"data_timestamp": data_timestamp, "loaded_through": data_timestamp[:10]}
    path.write_text(json.dumps(wm, indent=2))
    return wm


def since_date(watermark: dict, overlap_days: int = 2) -> str:
    """Start date for the next pull. Overlapping is safe because the ODS upserts by nct_id."""
    loaded = date.fromisoformat(watermark["loaded_through"])
    return (loaded - timedelta(days=overlap_days)).isoformat()


def is_new_data(watermark: dict | None, data_timestamp: str) -> bool:
    """False when the registry hasn't refreshed since our last full load — the run can be skipped."""
    return watermark is None or watermark.get("data_timestamp") != data_timestamp
