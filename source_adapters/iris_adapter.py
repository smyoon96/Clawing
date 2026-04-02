from __future__ import annotations

import re

import requests
from bs4 import BeautifulSoup

from .base import BaseAdapter, RunContext, UnifiedRow
from .utils import split_measurement


class IRISAdapter(BaseAdapter):
    source_key = "iris"

    def collect(self, query: str, ctx: RunContext) -> list[UnifiedRow]:
        url = "https://www.epa.gov/iris/search"
        params = {"search_api_fulltext": query}
        r = requests.get(url, params=params, timeout=ctx.timeout_sec)
        r.raise_for_status()

        html = r.text
        out_file = ctx.evidence_dir / f"{self.source_key}_{query}.html"
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text(html, encoding="utf-8")

        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text("\n", strip=True)

        rows: list[UnifiedRow] = []
        patterns = [
            ("Reference dose", r"Reference dose[^\n]{0,120}"),
            ("Reference concentration", r"Reference concentration[^\n]{0,120}"),
            ("Cancer", r"Cancer[^\n]{0,120}"),
        ]
        for endpoint, pat in patterns:
            for m in re.finditer(pat, text, re.I):
                chunk = m.group(0)
                cmp_, num, unit, qual = split_measurement(chunk)
                rows.append(
                    UnifiedRow(
                        source_name=self.source_key,
                        query_input=query,
                        cas_number=query,
                        substance_name=query,
                        endpoint=endpoint,
                        field_name="iris_summary",
                        raw_value=chunk,
                        comparator=cmp_,
                        numeric_value=num,
                        unit=unit,
                        qualifier=qual,
                        hazard_code="",
                        hazard_category="",
                        study_guideline="",
                        test_conditions="",
                        section_path="iris.search",
                        evidence_url=r.url,
                        evidence_file=str(out_file),
                        retrieved_at_utc=self.now_utc_iso(),
                    )
                )

        return rows
