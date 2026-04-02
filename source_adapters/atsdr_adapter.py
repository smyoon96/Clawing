from __future__ import annotations

import re
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .base import BaseAdapter, RunContext, UnifiedRow


class ATSDRAdapter(BaseAdapter):
    source_key = "atsdr"

    @staticmethod
    def _fetch(url: str, params: dict | None, timeout: float):
        full_url = f"{url}?{urlencode(params)}" if params else url
        req = Request(full_url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            final_url = resp.geturl()
        return body, final_url

    def collect(self, query: str, ctx: RunContext) -> list[UnifiedRow]:
        html, final_url = self._fetch("https://www.atsdr.cdc.gov/ToxProfiles/", None, ctx.timeout_sec)

        out_file = ctx.evidence_dir / f"{self.source_key}_{query}.html"
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text(html, encoding="utf-8")

        text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html)).strip()
        links = re.findall(r'href=["\']([^"\']+\.pdf)["\']', html, re.I)
        matched_pdf = ""
        for l in links:
            if query.lower() in l.lower():
                matched_pdf = l
                break

        rows = [
            UnifiedRow(
                source_name=self.source_key,
                query_input=query,
                cas_number=query,
                substance_name=query,
                endpoint="toxicological profile",
                field_name="atsdr_profile_hint",
                raw_value=(matched_pdf or " ".join(text.split()[:40])),
                comparator="",
                numeric_value="",
                unit="",
                qualifier="",
                hazard_code="",
                hazard_category="",
                study_guideline="",
                test_conditions="",
                section_path="atsdr.profile",
                evidence_url=final_url,
                evidence_file=str(out_file),
                retrieved_at_utc=self.now_utc_iso(),
            )
        ]
        return rows
