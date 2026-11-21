# Clinical-Trials Data Acquisition Pipeline

A Python pipeline that acquires clinical-trials data from the **ClinicalTrials.gov API v2**, normalizes it, and
builds a **SQLite database + Chroma vector index** — ready for a Retrieval-Augmented Generation (RAG) chatbot to
answer grounded questions about real clinical trials.

> **Note:** This project uses the public API for reliability. Large-scale web scraping (Playwright + anti-bot
> handling) is a separate skill showcased elsewhere.

## What it demonstrates
- **Data acquisition** from a real public API, with paging and retry/backoff
- **ETL / normalization** into a clean, queryable schema
- **Vector search / RAG data prep** (embeddings → Chroma)
- **CI/CD** — scheduled refresh via GitHub Actions
- **Cloud (optional)** — artifact upload to AWS S3

## Architecture
```
ClinicalTrials.gov API v2 → fetch → normalize → SQLite (trials.db)
                                            └──→ Chroma (vector index) → RAG chatbot
GitHub Actions (cron) refreshes the data · optional S3 upload
```
See [docs/design.md](docs/design.md) for details.

## Quickstart
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Fetch + normalize + store + embed for a condition
python -m src.pipeline --condition "lung cancer" --max 100

# Outputs:
#   data/trials.db     (SQLite: structured trial records)
#   data/chroma/       (Chroma: vector index for retrieval)
```

## Connecting your RAG chatbot
The pipeline produces a stable interface your existing chatbot reads:
- `data/trials.db` — SQLite `trials` table (nct_id, title, condition, phase, status, summary, …)
- `data/chroma/` — Chroma collection `clinical_trials` (one document per trial + metadata)

Point your chatbot's retriever at `data/chroma/` and answer questions like:
> "What Phase 3 trials are studying pembrolizumab for lung cancer?"

## Status
🚧 Scaffold — see [docs/design.md](docs/design.md) and milestones. Built with TDD.
