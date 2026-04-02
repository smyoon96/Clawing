from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass
class RunContext:
    evidence_dir: Path
    timeout_sec: float = 20.0
    debug: bool = False


@dataclass
class UnifiedRow:
    source_name: str
    query_input: str
    cas_number: str
    substance_name: str
    endpoint: str
    field_name: str
    raw_value: str
    comparator: str
    numeric_value: str
    unit: str
    qualifier: str
    hazard_code: str
    hazard_category: str
    study_guideline: str
    test_conditions: str
    section_path: str
    evidence_url: str
    evidence_file: str
    retrieved_at_utc: str


class BaseAdapter:
    source_key: str = "base"

    def collect(self, query: str, ctx: RunContext) -> list[UnifiedRow]:
        raise NotImplementedError

    @staticmethod
    def now_utc_iso() -> str:
        return datetime.now(timezone.utc).isoformat()
