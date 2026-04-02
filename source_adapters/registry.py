from __future__ import annotations

from .base import BaseAdapter
from .iris_adapter import IRISAdapter


def build_registry() -> dict[str, BaseAdapter]:
    return {
        "iris": IRISAdapter(),
    }
