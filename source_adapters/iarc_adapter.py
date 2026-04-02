from __future__ import annotations

import re
from urllib.request import Request, urlopen

from .base import BaseAdapter, RunContext, UnifiedRow


class IARCAdapter(BaseAdapter):
    source_key = "iarc"

    @staticmethod
    def _fetch(url: str, timeout: float):
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            final_url = resp.geturl()
        return body, final_url

    def collect(self, query: str, ctx: RunContext) -> list[UnifiedRow]:
        html, final_url = self._fetch("https://monographs.iarc.who.int/", ctx.timeout_sec)
        out_file = ctx.evidence_dir / f"{self.source_key}_{query}.html"
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text(html, encoding="utf-8")

        text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html)).strip()
        m = re.search(r"Group\s*[12AB]+", text, re.I)
        group = m.group(0) if m else ""

        return [
            UnifiedRow(
                source_name=self.source_key,
                query_input=query,
                cas_number=query,
                substance_name=query,
                endpoint="carcinogenicity",
                field_name="iarc_group_hint",
                raw_value=group or " ".join(text.split()[:40]),
                comparator="",
                numeric_value="",
                unit="",
                qualifier="",
                hazard_code="",
                hazard_category=group,
                study_guideline="",
                test_conditions="",
                section_path="iarc.monograph",
                evidence_url=final_url,
                evidence_file=str(out_file),
                retrieved_at_utc=self.now_utc_iso(),
            )
        ]
