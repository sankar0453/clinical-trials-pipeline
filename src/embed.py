"""Embed trials and write them to a persistent Chroma collection for RAG retrieval.

TODO (build with TDD — see tests/test_embed.py):
  - build_index(trials, path): for each Trial, add its `embed_text` to a Chroma collection
    named `clinical_trials`, with metadata {nct_id, condition, phase, status} and id=nct_id.
  - Default embedder: sentence-transformers "all-MiniLM-L6-v2" (local, free).
    Optionally switch to OpenAI embeddings if OPENAI_API_KEY is set.
  - Idempotent: upsert by id so re-runs don't duplicate.

The chatbot reads this collection from `data/chroma/`.
"""
from __future__ import annotations

DEFAULT_CHROMA_PATH = "data/chroma"
COLLECTION = "clinical_trials"
DEFAULT_MODEL = "all-MiniLM-L6-v2"


def build_index(trials: list, path: str = DEFAULT_CHROMA_PATH, model: str = DEFAULT_MODEL) -> int:
    """Embed trials into a persistent Chroma collection; return count. IMPLEMENT ME (TDD)."""
    raise NotImplementedError(
        "Use chromadb.PersistentClient(path); get_or_create_collection(COLLECTION); "
        "collection.upsert(ids=[nct_id], documents=[embed_text], metadatas=[{...}]). "
        "Embeddings via chromadb SentenceTransformerEmbeddingFunction(model) or OpenAI."
    )
