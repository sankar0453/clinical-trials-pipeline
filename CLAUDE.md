# CLAUDE.md — Clinical-Trials Data Acquisition Pipeline


**Goal:** a small, finished, impressive project — NOT production. Scope for 2–3 weekends. Small-and-done beats
big-and-abandoned. When in doubt, cut scope.

---

## What this builds
A **production-style, AWS-native** daily batch pipeline: pull clinical-trials data from the **ClinicalTrials.gov
API v2** → land in an **S3 data lake** (raw JSON → curated Parquet) → load into an **RDS Postgres ODS** →
optionally into **Redshift** for analytics — orchestrated by **AWS Step Functions** + **EventBridge Scheduler**,
with **Glue Python Shell jobs** as compute (no Lambda). Built **incrementally as a learning project**; paid infra
is torn down after each session via **Terraform**. Full spec in `docs/design.md`.

## Design decisions (locked — from brainstorming)
| Decision | Choice | Why |
|----------|--------|-----|
| Data source | **ClinicalTrials.gov API v2** | Free, public, no scraping needed. |
| Freshness | **Daily batch** | Matches how the registry updates; streaming would be wrong. |
| Orchestration | **AWS Step Functions** (user's choice) | AWS-native; triggers Glue, **not Lambda** (blocked on account). |
| Compute | **AWS Glue Python Shell jobs** | Serverless Python; Step Functions `StartJobRun`. No Lambda. |
| Schedule | **EventBridge Scheduler** | Daily trigger. |
| Data lake | **S3** raw (JSON) → curated (Parquet) | Partitioned by date; medallion layers. |
| ODS | **RDS Postgres** (free tier); local Postgres for early dev | Upsert by nct_id. |
| Warehouse | **Redshift Serverless** | Analytics; spin up → tear down. |
| Alerts | **SNS** | Email on failure. |
| IaC | **Terraform** | Provision + `destroy` for cost control. |
| Chatbot | reuse existing Python RAG chatbot | Optional Stage 8 consumer. |

## ⚠️ FIRST THING in the new session — verify AWS access (Stage 0)
The account **can't use Lambda**, so it may be restricted. Before building anything, confirm **Glue, Step
Functions, RDS, Redshift Serverless, EventBridge, SNS, S3, IAM** are usable on the account. If blocked, revisit
the plan. See `docs/design.md` → Prerequisite.

## How to build (recommended flow for the new session)
1. **Stage 0:** verify AWS service access + create a least-privilege IAM role.
2. Invoke **superpowers:writing-plans** to turn `docs/design.md` into a step-by-step plan for the current stage.
3. Build **one stage per session** (0→8), with **TDD** for the Python modules (test in `tests/`, code in `src/`).
4. Build order: local logic (`fetch`→`normalize`) → S3 zones → ODS → Glue jobs → Step Functions+schedule →
   Redshift → Terraform → (optional) RAG chatbot.
5. **End each paid session with `terraform destroy`** on Redshift/RDS to control cost.

## Commands
```bash
# Setup (needs Python 3.10+; macOS system python3 is 3.9 — use e.g. /opt/homebrew/bin/python3.12)
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Ingest (writes RAW pages + _manifest.json under data/…/date=YYYY-MM-DD/)
python -m src.fetch --condition "lung cancer" --max 50 --out data/sample   # sample / smoke run
python -m src.fetch --incremental --since 2026-11-19                       # first incremental run, seeds watermark
python -m src.fetch --incremental                                          # daily run; skips if registry unchanged

# Tests (pytest.ini puts the repo root on sys.path; no network needed)
pytest
pytest tests/test_fetch.py -k retry   # subset
```
Walkthrough for demos/interviews: `docs/sample-run.md`. Whole-pipeline design: `docs/hld.md` (high level) and
`docs/lld.md` (schemas, state machine ASL, IAM, network, Terraform). Glue Python Shell runs Python 3.9, so keep
`src/` 3.9-compatible even though local dev uses 3.12.
There is no `src/pipeline.py` yet, despite the README/`.github/workflows/pipeline.yml` referencing
`python -m src.pipeline` — that CLI entrypoint hasn't been built. Don't assume it exists.

## Notes on the existing scaffold
- `src/fetch.py` + `src/state.py` are **done and tested**: incremental ingest (LastUpdatePostDate watermark,
  `/version` freshness gate, full records, pageSize 1000, Retry-After/jitter, count reconciliation, one file per
  page + manifest). Reuse `fetch.ingest()` inside the Glue ingest job; swap the local paths for S3 in Stage 2.
  Strategy rationale: `docs/design.md` → "Ingest strategy".
- `src/normalize.py`, `store.py`, `embed.py` are stubs from an earlier local-only (SQLite/Chroma) plan — each
  function body is `raise NotImplementedError(...)`. In this AWS direction: keep `normalize.py`; `store.py` →
  repurpose toward S3/Parquet + RDS (`to_s3.py`, `load_ods.py`); `embed.py`/Chroma move to the **optional Stage 8**
  RAG consumer. `.github/workflows/pipeline.yml` (GitHub Actions) is now just CI for tests — scheduling is
  EventBridge, not Actions.
- `tests/` has `test_fetch.py` and `test_state.py` (fake HTTP session, no network). `normalize` tests are next.
- `.github/workflows/pipeline.yml` now just runs `pytest` on push/PR.
- `README.md` still describes the earlier local-only SQLite/Chroma architecture (`src.pipeline`, `data/trials.db`,
  `data/chroma/`) and is stale relative to the AWS direction above — don't treat it as current design, only this
  file and `docs/design.md` are authoritative. Updating the README to match is in-scope whenever it's touched.

## Conventions
- Python 3.10+. Deps in `requirements.txt`. Keep each `src/` module single-purpose (see design doc).
- No secrets in code — use env vars (`OPENAI_API_KEY`, AWS creds) via `.env` (gitignored).
- Honest README: state that scraping is showcased separately; this uses the API for reliability.

## Resume payoff (what this lets him claim, truthfully)
Data acquisition · ETL/normalization · SQLite · vector search / RAG data prep · CI/CD (GitHub Actions) · AWS S3 ·
Python. Fills the **RAG gap** flagged in `../Resumes/02-interview-prep/rag.md`.
