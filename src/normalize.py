"""Normalize a raw ClinicalTrials.gov study into a flat, clean record.

TODO (build with TDD — see tests/test_normalize.py):
  - Map the nested `protocolSection` fields into the flat schema below.
  - Clean whitespace; join list fields (conditions, interventions) into readable strings.
  - Build `embed_text` = title + summary + condition + intervention (what the chatbot retrieves on).
"""
from __future__ import annotations

from dataclasses import dataclass, asdict


@dataclass
class Trial:
    nct_id: str
    title: str
    condition: str
    intervention: str
    phase: str
    status: str
    sponsor: str
    start_date: str
    summary: str
    embed_text: str

    def to_dict(self) -> dict:
        return asdict(self)


def normalize(raw: dict) -> Trial:
    """Convert one raw study dict into a Trial. IMPLEMENT ME (TDD)."""
    raise NotImplementedError(
        "Implement normalize(): pull fields from raw['protocolSection'] "
        "(identificationModule, conditionsModule, armsInterventionsModule, "
        "designModule, statusModule, sponsorCollaboratorsModule, descriptionModule)."
    )
