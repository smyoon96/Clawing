from __future__ import annotations

import re


def split_measurement(value: str) -> tuple[str, str, str, str]:
    raw = re.sub(r"\s+", " ", value or "").strip()
    if not raw:
        return "", "", "", ""

    m = re.search(
        r"(?P<cmp><=|>=|<|>|=|≤|≥)?\s*(?P<num>\d+(?:[.,]\d+)?)\s*(?P<unit>[A-Za-zμµ%/·²³0-9\-]+)?\s*(?P<qual>.*)",
        raw,
    )
    if not m:
        return "", "", "", raw

    return (
        (m.group("cmp") or "").strip(),
        (m.group("num") or "").replace(",", ".").strip(),
        (m.group("unit") or "").strip(),
        (m.group("qual") or "").strip(" ;,"),
    )


def extract_hazard_codes(text: str) -> list[str]:
    return sorted(set(re.findall(r"\b(?:H|EUH)\d{3}\b", text or "")))
