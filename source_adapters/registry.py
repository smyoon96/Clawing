from __future__ import annotations

from .base import BaseAdapter
from .hcis_adapter import HCISAdapter
from .ipcs_adapter import IPCSAdapter


def build_registry() -> dict[str, BaseAdapter]:
    return {
        "hcis": HCISAdapter(),
        "ipcs": IPCSAdapter(),
    }
