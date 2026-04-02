from __future__ import annotations

from .base import BaseAdapter
from .iris_adapter import IRISAdapter
from .hcis_adapter import HCISAdapter
from .atsdr_adapter import ATSDRAdapter
from .iarc_adapter import IARCAdapter


def build_registry() -> dict[str, BaseAdapter]:
    return {
        "iris": IRISAdapter(),
        "hcis": HCISAdapter(),
        "atsdr": ATSDRAdapter(),
        "iarc": IARCAdapter(),
    }
