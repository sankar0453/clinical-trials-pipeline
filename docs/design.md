# Design — Production Clinical-Trials Data Pipeline (AWS Step Functions)

## Purpose
Acquire clinical-trials data from ClinicalTrials.gov daily, land it in an S3 data lake (raw → curated), load it
into an operational data store (RDS Postgres), and optionally into a Redshift warehouse for analytics — all
orchestrated by **AWS Step Functions** on a daily schedule. Built **incrementally as a learning project**; paid
resources are torn down after each session via Terraform.

Detailed designs: [hld.md](hld.md) (architecture, flows, non-functional) · [lld.md](lld.md) (modules, schemas,
state machine, IAM, network, Terraform) · [sample-run.md](sample-run.md) (local ingest walkthrough).

## Scope & decisions (locked)
| Decision | Choice | Notes |
|----------|--------|-------|
| Freshness | **Daily batch** | Matches how ClinicalTrials.gov updates. Streaming would be wrong here. |
| Orchestration | **AWS Step Functions** | Triggers **Glue** jobs |
| Compute | **AWS Glue Python Shell jobs** | Serverless Python; cheap; Step Functions can `StartJobRun` directly. |
| Scheduling | **EventBridge Scheduler** | Daily cron trigger for the state machine. |
| Data lake | **S3**: raw (JSON) → curated (Parquet) | Partitioned by ingest date; medallion layers. |
| ODS | **RDS Postgres** (free tier) — local Postgres for early dev | Current-state, upsert by nct_id. |
| Warehouse | **Redshift Serverless** | Spin up for demo, pause/destroy after. |
| Alerts | **SNS** | Email on pipeline failure. |
| IaC | **Terraform** | `apply` to provision, `destroy` to control cost. |
| Chatbot (optional) | reuse existing Python RAG chatbot | Consumes curated/ODS data. Stage 8. |

## ⚠️ Prerequisite (Stage 0) — verify account access
The AWS account which suggests it may be restricted. BEFORE building, confirm these are
usable on the account (console or CLI): **Glue, Step Functions, RDS, Redshift Serverless, EventBridge, SNS, S3,
IAM role creation.** If any are blocked, revisit the plan (e.g. run Glue-equivalent logic on a local/EC2 runner,
or use a different/personal-sandbox account). Do not start Stage 1 until this passes.

## Target architecture
```
EventBridge Scheduler (daily)
      │ StartExecution
      ▼
Step Functions state machine
   State 1: Glue "ingest"     → s3://<bucket>/raw/date=YYYY-MM-DD/*.json     (immutable)
   State 2: Glue "transform"  → s3://<bucket>/curated/*.parquet             (clean, typed)
   State 3: Glue "load-ods"   → RDS Postgres (upsert by nct_id)
   State 4: Redshift COPY      from s3 curated                              (optional analytics)
   Catch  : any failure       → SNS notify (email)
   Retry  : transient errors  → backoff per state
```

## Ingest strategy (how we pull from the API)
A daily job should pull **only what changed**, be **safe to re-run**, and **prove it got everything**.

| Concern | Approach |
|---------|----------|
| What to pull | **Incremental**: `filter.advanced=AREA[LastUpdatePostDate]RANGE[<since>,MAX]` (whole registry, or `query.cond` to scope). ~1k trials change per day vs 500k+ total. |
| Where `since` comes from | **Watermark** (`src/state.py`): `{"data_timestamp", "loaded_through"}`; local JSON file now, S3 object in AWS. `since = loaded_through − 2 days` overlap — safe because the ODS upserts by `nct_id`. First run is seeded with `--since`. |
| Skip no-op runs | `GET /api/v2/version` → `dataTimestamp` (registry refreshes ~daily). Unchanged vs watermark → exit early. |
| Watermark advance | Only after a **complete** delta (count reconciled, no `--max` cap). A failed run re-pulls the same window next time. |
| Payload | **Full study records** in RAW (no `fields=`), so later stages (eligibility, locations, RAG) never need a re-fetch. `normalize.py` selects the curated fields. |
| Paging | `pageSize=1000` (API max), follow `nextPageToken`. |
| Rate limit & retry | One `requests.Session`; ~1.3 s between pages (~45 req/min, under the ~50/min limit); on 429/5xx honour `Retry-After`, else `2^i + jitter`; 4xx fails fast. |
| Completeness | `countTotal=true` on the first page → `totalCount`; `received != expected` raises `CountMismatchError` → Step Functions Catch → SNS. |
| Output | One file per page as it arrives (`raw/date=YYYY-MM-DD/page-NNNN.json`) + `_manifest.json` (data version, since, counts, files). Flat memory; partial runs are inspectable. |

Alternatives considered: **AACT** (CTTI's daily Postgres mirror) is easier but skips the acquisition skill this
project showcases; a **full bulk download** is right only for a one-time backfill, not daily.

## Data zones & schema
- **RAW** (`s3://.../raw/date=YYYY-MM-DD/`): exact API JSON, immutable, partitioned by ingest date.
- **CURATED** (`s3://.../curated/`): Parquet, flat schema, one row per trial.
- **ODS** (`trials` table, Postgres): current state, upsert on `nct_id`.
- **Fields:** nct_id (PK), title, condition, intervention, phase, status, sponsor, start_date, summary.
- **Redshift:** same curated data, loaded via COPY for BI/analytics queries.

## Modules (Python; run locally first, then lifted into Glue jobs)
| File | Stage | Does |
|------|-------|------|
| `src/fetch.py` | 1 | Incremental ingest from API v2 → RAW pages + manifest (see Ingest strategy). **Done, tested.** |
| `src/state.py` | 1 | Watermark read/write, overlap window, "has the registry refreshed?" check. **Done, tested.** |
| `src/normalize.py` | 1 | Map raw JSON → flat record; build curated rows. (stub → implement) |
| `src/to_s3.py` | 2 | Write raw JSON + curated Parquet to S3 (boto3 / pyarrow). (new) |
| `src/load_ods.py` | 3 | Upsert curated rows into RDS Postgres. (new) |
| Glue job scripts | 4 | Thin wrappers that call the above inside Glue Python Shell. (new, `glue/`) |
| `infra/` (Terraform) | 5–7 | S3, IAM, Glue jobs, Step Functions, EventBridge, RDS, Redshift, SNS. (new) |

## Incremental build stages
0. **Verify AWS access** (prerequisite above) + create an IAM role/user with least-privilege for the services.
1. **Local logic:** `fetch` + `normalize` working locally, tested. Produces curated rows in memory/CSV.
2. **S3 zones:** upload raw JSON + curated Parquet to S3, partitioned by date. Learn boto3, Parquet, layout.
3. **ODS:** load curated → Postgres (start local Docker Postgres, then RDS free-tier). Idempotent upsert.
4. **Glue jobs:** package the Python as Glue Python Shell jobs; run each manually in AWS.
5. **Step Functions + schedule:** state machine chaining the Glue jobs; EventBridge daily trigger; SNS alert.
6. **Redshift:** Serverless workgroup; COPY curated from S3; run analytics queries; pause/destroy after.
7. **Terraform:** codify all infra; `apply`/`destroy` to provision and control cost.
8. **(optional) RAG chatbot:** point the existing chatbot at curated/ODS (or a Chroma index built from curated).

## Cost control (important for a learning project)
- **Glue Python Shell**: billed per second (~$0.44/DPU-hr, small jobs = cents). 
- **RDS**: free tier (db.t3/t4g.micro, 750 hrs/mo, 12 months) — or local Postgres to pay nothing.
- **Redshift Serverless**: use trial credits; **pause or `terraform destroy` when not demoing**.
- **Step Functions / EventBridge / SNS**: effectively free at this volume.
- **Rule:** end each session with `terraform destroy` on the expensive parts (Redshift, RDS if not free-tier).

## Testing
- `fetch` / `state`: fake HTTP session (no network) — paging, page-size cap, pacing, Retry-After, backoff,
  fail-fast on 4xx, manifest + count reconciliation, watermark round-trip/overlap/skip (`tests/test_fetch.py`,
  `tests/test_state.py`).
- `normalize`: fixture → flat schema.
- `load_ods`: temp Postgres, assert idempotent upsert.
- Glue/Step Functions: validate state-machine definition; smoke-test one end-to-end run with `--max 50`.

## Risks
- **Account restrictions** (Stage 0) — biggest risk; verify first.
- **Cost creep** — mitigate with Terraform destroy + free tiers + serverless.
- **Scope** — build one stage per session; don't chase all 8 at once.
