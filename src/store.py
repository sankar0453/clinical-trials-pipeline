"""Persist normalized trials into SQLite (idempotent upsert on nct_id).

TODO (build with TDD — see tests/test_store.py):
  - init_db(path): create the `trials` table if absent.
  - upsert(trials, path): INSERT OR REPLACE each Trial keyed by nct_id (re-runs must not duplicate).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

DEFAULT_DB = "data/trials.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS trials (
    nct_id       TEXT PRIMARY KEY,
    title        TEXT,
    condition    TEXT,
    intervention TEXT,
    phase        TEXT,
    status       TEXT,
    sponsor      TEXT,
    start_date   TEXT,
    summary      TEXT,
    embed_text   TEXT
);
"""


def init_db(path: str = DEFAULT_DB) -> sqlite3.Connection:
    """Create the DB + schema and return a connection. IMPLEMENT ME (TDD)."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    raise NotImplementedError("Open sqlite3 connection, execute SCHEMA, return conn.")


def upsert(trials: list, path: str = DEFAULT_DB) -> int:
    """Insert-or-replace trials; return count written. IMPLEMENT ME (TDD)."""
    raise NotImplementedError("INSERT OR REPLACE INTO trials(...) VALUES(...) per Trial.")
