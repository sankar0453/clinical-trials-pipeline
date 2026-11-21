"""Ingest clinical-trials records from the ClinicalTrials.gov API v2 into the RAW zone.

Design (see docs/design.md → "Ingest strategy"):
  - Incremental: ask only for trials whose LastUpdatePostDate >= a watermark date (minus overlap).
  - Freshness gate: /api/v2/version's dataTimestamp tells us if the registry refreshed since last run.
  - Full records: the RAW zone keeps the complete study JSON; normalize.py picks fields later.
  - pageSize 1000 (API max), polite pacing (~45 req/min), retry with Retry-After + jittered backoff.
  - Each page is written to its own file as it arrives; a _manifest.json records counts for reconciliation.

Examples:
    python -m src.fetch --condition "lung cancer" --max 50            # sample run
    python -m src.fetch --since 2026-11-19                            # one-off delta, whole registry
    python -m src.fetch --incremental --since 2026-11-19              # first incremental run (seeds watermark)
    python -m src.fetch --incremental                                 # daily run (uses watermark)
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable, Iterator

import requests

from src import state

API_URL = "https://clinicaltrials.gov/api/v2/studies"
VERSION_URL = "https://clinicaltrials.gov/api/v2/version"
MAX_PAGE_SIZE = 1000
MIN_REQUEST_INTERVAL = 1.3  # seconds between requests → ~45/min, under the ~50/min limit
RETRYABLE = {429, 500, 502, 503, 504}
USER_AGENT = "clinical-trials-pipeline/0.1"

Sleep = Callable[[float], None]


class CountMismatchError(RuntimeError):
    """Received a different number of studies than the API said exist."""


@dataclass
class Page:
    studies: list[dict]
    total_count: int | None


def new_session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    return s


def build_params(
    condition: str | None = None,
    updated_since: str | None = None,
    page_size: int = MAX_PAGE_SIZE,
    fields: list[str] | None = None,
) -> dict:
    """Query params for /studies. No `fields` → full study records (what the RAW zone stores)."""
    params: dict = {"pageSize": min(page_size, MAX_PAGE_SIZE), "countTotal": "true"}
    if condition:
        params["query.cond"] = condition
    if updated_since:
        params["filter.advanced"] = f"AREA[LastUpdatePostDate]RANGE[{updated_since},MAX]"
    if fields:
        params["fields"] = ",".join(fields)
    return params


def get_with_retry(session, url: str, params: dict, attempts: int = 5, sleep: Sleep = time.sleep) -> dict:
    """GET JSON; on 429/5xx wait (Retry-After if given, else 2^i + jitter) and retry."""
    for i in range(attempts):
        resp = session.get(url, params=params, timeout=60)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code in RETRYABLE and i < attempts - 1:
            retry_after = resp.headers.get("Retry-After")
            sleep(float(retry_after) if retry_after else 2**i + random.random())
            continue
        resp.raise_for_status()
    raise RuntimeError("unreachable")


def get_data_timestamp(session=None) -> str:
    """The registry's current data version, e.g. '2026-11-20T10:00:05' (refreshes ~daily)."""
    session = session or new_session()
    return get_with_retry(session, VERSION_URL, {})["dataTimestamp"]


def iter_pages(
    params: dict, max_records: int | None = None, session=None, sleep: Sleep = time.sleep
) -> Iterator[Page]:
    """Yield pages until the API runs out (no nextPageToken) or max_records is reached."""
    session = session or new_session()
    params = dict(params)
    page_size = params.get("pageSize", MAX_PAGE_SIZE)
    fetched = 0
    first = True
    while max_records is None or fetched < max_records:
        if max_records is not None:
            params["pageSize"] = min(page_size, max_records - fetched)
        if not first:
            sleep(MIN_REQUEST_INTERVAL)
        data = get_with_retry(session, API_URL, params, sleep=sleep)
        studies = data.get("studies", [])
        if max_records is not None:
            studies = studies[: max_records - fetched]
        if not studies:
            return
        yield Page(studies=studies, total_count=data.get("totalCount"))
        fetched += len(studies)
        first = False

        token = data.get("nextPageToken")
        if not token:
            return
        params["pageToken"] = token
        params.pop("countTotal", None)  # only needed once


def ingest(
    out_dir: str | Path,
    run_date: str | None = None,
    condition: str | None = None,
    updated_since: str | None = None,
    max_records: int | None = None,
    data_timestamp: str | None = None,
    session=None,
    sleep: Sleep = time.sleep,
) -> dict:
    """Write RAW pages to <out_dir>/date=<run_date>/page-NNNN.json plus _manifest.json.

    Raises CountMismatchError if we received fewer/more studies than the API reported.
    """
    run_date = run_date or date.today().isoformat()
    run_dir = Path(out_dir) / f"date={run_date}"
    run_dir.mkdir(parents=True, exist_ok=True)

    params = build_params(condition=condition, updated_since=updated_since)
    total_count = None
    received = 0
    files = []
    for n, page in enumerate(iter_pages(params, max_records, session=session, sleep=sleep), start=1):
        if total_count is None:
            total_count = page.total_count
        name = f"page-{n:04d}.json"
        (run_dir / name).write_text(json.dumps(page.studies))
        files.append(name)
        received += len(page.studies)

    total_count = total_count or 0
    expected = min(total_count, max_records) if max_records is not None else total_count
    if received != expected:
        raise CountMismatchError(f"expected {expected} studies, received {received}")

    manifest = {
        "run_date": run_date,
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "data_timestamp": data_timestamp,
        "condition": condition,
        "updated_since": updated_since,
        "max_records": max_records,
        "total_count": total_count,
        "expected": expected,
        "received": received,
        "files": files,
    }
    (run_dir / "_manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def _cli(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Ingest ClinicalTrials.gov studies into the RAW zone")
    p.add_argument("--condition", help="limit to a condition, e.g. 'lung cancer'")
    p.add_argument("--since", help="only trials updated on/after YYYY-MM-DD")
    p.add_argument("--incremental", action="store_true", help="use/advance the watermark in --state")
    p.add_argument("--state", default="data/state/watermark.json")
    p.add_argument("--overlap-days", type=int, default=2)
    p.add_argument("--max", type=int, dest="max_records", help="cap records (sample/smoke runs)")
    p.add_argument("--out", default="data/raw")
    p.add_argument("--run-date", help="partition date (default: today)")
    args = p.parse_args(argv)

    session = new_session()
    data_ts = get_data_timestamp(session)
    print(f"Registry data version: {data_ts}")

    since = args.since
    if args.incremental:
        wm = state.read_watermark(args.state)
        if not state.is_new_data(wm, data_ts):
            print("Registry has not refreshed since the last load — nothing to do.")
            return 0
        if wm:
            since = state.since_date(wm, args.overlap_days)
        elif not since:
            p.error("no watermark yet — seed the first incremental run with --since YYYY-MM-DD")
        print(f"Incremental pull: trials updated since {since}")

    manifest = ingest(
        out_dir=args.out, run_date=args.run_date, condition=args.condition, updated_since=since,
        max_records=args.max_records, data_timestamp=data_ts, session=session,
    )
    run_dir = Path(args.out) / f"date={manifest['run_date']}"
    print(f"Wrote {manifest['received']} studies in {len(manifest['files'])} page file(s) → {run_dir}")
    print(f"Reconciled: API totalCount={manifest['total_count']}, expected={manifest['expected']}, "
          f"received={manifest['received']}")

    if args.incremental and args.max_records is None:  # only a complete delta advances the watermark
        wm = state.write_watermark(args.state, data_ts)
        print(f"Watermark advanced → {wm['loaded_through']} ({args.state})")
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
