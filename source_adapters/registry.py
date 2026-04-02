from __future__ import annotations

from .base import BaseAdapter
from .hcis_adapter import HCISAdapter


def build_registry() -> dict[str, BaseAdapter]:
    return {
        "hcis": HCISAdapter(),
    }
