# Sample run — ingest walkthrough

How to run the ingest step on sample data, and what to say at each step in a technical discussion.
Everything here runs on a laptop for free: no AWS account, no API key. Outputs below are from a real run on
2026-11-21.

## What you need
| Requirement | Why |
|-------------|-----|
| Python **3.10+** (tested on 3.12) | macOS's bundled `python3` is 3.9. Use Homebrew's `python3.12`. |
| Internet access to `clinicaltrials.gov` | Public API v2. No key, no sign-up. |
| `pip install -r requirements.txt` | Ingest only needs `requests`; `pytest` for tests. boto3/pyarrow/psycopg2 are for later stages. |
| ~1 MB disk (sample), ~40 MB (2-day delta) | Full study records are ~20 KB each. |

## Step 1 — Set up
```bash
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Step 2 — Run the tests (no network)
```bash
pytest -q          # 17 passed
```
**Talking point:** HTTP is replaced by a small fake session, so the tests run offline and finish in
milliseconds. They cover: following page tokens, the max-records cap, the pause between requests,
`Retry-After` on 429, backoff with jitter on 5xx, failing fast on 4xx, the count check, and the watermark logic.

## Step 3 — Sample run (small, capped)
```bash
python -m src.fetch --condition "lung cancer" --max 50 --out data/sample
```
```
Registry data version: 2026-11-20T10:00:05
Wrote 50 studies in 1 page file(s) → data/sample/date=2026-11-21
Reconciled: API totalCount=14749, expected=50, received=50
```
**Talking points:**
1. **Freshness check first.** `GET /api/v2/version` returns the registry's `dataTimestamp`, which the registry
   refreshes about once a day. This is the data version we're loading.
2. **Query.** `query.cond=lung cancer`, `pageSize` capped by `--max`, and `countTotal=true` so the API also
   reports how many trials match (14,749).
3. **Count check.** Expected = min(total, cap) = 50, received = 50. A mismatch fails the run.

Output layout (this is the RAW zone; in Stage 2 the same layout goes to `s3://<bucket>/raw/`):
```
data/sample/date=2026-11-21/
├── page-0001.json      # full study records, exactly as the API returned them
└── _manifest.json      # run metadata, used for checks and lineage
```
`_manifest.json`:
```json
{ "run_date": "2026-11-21", "data_timestamp": "2026-11-20T10:00:05", "condition": "lung cancer",
  "updated_since": null, "max_records": 50, "total_count": 14749, "expected": 50, "received": 50,
  "files": ["page-0001.json"] }
```
Each record keeps every module: identification, status, sponsor, description, conditions, design,
arms/interventions, outcomes, **eligibility**, **contacts/locations**, and more. **Why the full record:** the raw
layer is the source of truth. `normalize.py` (next) picks the 9 curated fields; RAG and analytics can use more
later without re-calling the API.

## Step 4 — First incremental run (seed the watermark)
```bash
python -m src.fetch --incremental --since 2026-11-19
```
```
Incremental pull: trials updated since 2026-11-19
Wrote 2032 studies in 3 page file(s) → data/raw/date=2026-11-21
Reconciled: API totalCount=2032, expected=2032, received=2032
Watermark advanced → 2026-11-20 (data/state/watermark.json)
```
**Talking points:**
- **Delta, not full reload.** The filter `AREA[LastUpdatePostDate]RANGE[2026-11-19,MAX]` returns only trials
  updated since that date. That's about 2,000 trials out of 500k+ in the registry.
- **Paging.** `pageSize=1000` (the API maximum) means 3 requests. The job follows `nextPageToken`, waits ~1.3 s
  between requests to stay under the ~50 requests/min limit, and writes each page to disk as it arrives.
- **Watermark** (`data/state/watermark.json`; an S3 object in AWS):
  `{"data_timestamp": "2026-11-20T10:00:05", "loaded_through": "2026-11-20"}`. It only moves forward after a
  **complete** delta with a matching count.

## Step 5 — Re-run (the daily job)
```bash
python -m src.fetch --incremental
```
```
Registry data version: 2026-11-20T10:00:05
Registry has not refreshed since the last load — nothing to do.
```
**Talking point:** the run skips itself because the registry hasn't changed. After the next refresh it pulls
`since = loaded_through − 2 days`. The overlap is safe because the database load inserts or updates by `nct_id`,
so re-pulling a trial does no harm. This is the command EventBridge → Step Functions → Glue runs every day.

## Failure handling (for "what if…?" questions)
| Scenario | Behaviour |
|----------|-----------|
| 429 rate-limited | Waits as long as the `Retry-After` header says, then retries (up to 5 attempts). |
| 5xx / brief outage | Backoff `2^i + random jitter` seconds, then retries. |
| 4xx (bad query) | Fails immediately; retrying won't help. |
| Fewer records than `totalCount` | `CountMismatchError`. In AWS: Step Functions Catch → SNS email. |
| Run dies halfway | Pages already written stay on disk for inspection; the watermark hasn't moved, so the next run re-pulls the same window. |

## Where this sits in the full pipeline
```
EventBridge (daily) → Step Functions
  1. Glue ingest    = this step (fetch.ingest)  → s3 raw/date=…/page-*.json + _manifest.json
  2. Glue transform = normalize.py              → s3 curated/*.parquet
  3. Glue load-ods  = load_ods.py               → RDS Postgres (insert or update by nct_id)
  4. Redshift COPY                              → analytics
  Catch → SNS email
```
Built so far: step 1 (locally). Next: `normalize.py` (Stage 1), then S3 (Stage 2).
