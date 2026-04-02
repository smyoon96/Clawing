from __future__ import annotations

import re
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .base import BaseAdapter, RunContext, UnifiedRow
from .utils import extract_hazard_codes


class HCISAdapter(BaseAdapter):
    source_key = "hcis"

    @staticmethod
    def _fetch(url: str, params: dict | None, timeout: float):
        full_url = f"{url}?{urlencode(params)}" if params else url
        req = Request(full_url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            final_url = resp.geturl()
        return body, final_url

    def collect(self, query: str, ctx: RunContext) -> list[UnifiedRow]:
        # HCIS 검색 페이지(쿼리 URL 변경 가능성 대비 2단계)
        candidates = [
            ("https://hcis.safeworkaustralia.gov.au/HazardousChemical", {"Search": query}),
            ("https://hcis.safeworkaustralia.gov.au/", None),
        ]

        html = ""
        final_url = ""
        for u, p in candidates:
            try:
                body, got = self._fetch(u, p, ctx.timeout_sec)
                html, final_url = body, got
                if query.lower() in body.lower() or "Hazardous" in body:
                    break
            except Exception:
                continue

        if not html:
            raise RuntimeError("HCIS 접근 실패")

        out_file = ctx.evidence_dir / f"{self.source_key}_{query}.html"
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text(html, encoding="utf-8")

        text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html)).strip()
        rows: list[UnifiedRow] = []

        # hazard code 우선 추출
        for code in extract_hazard_codes(text):
            rows.append(
                UnifiedRow(
                    source_name=self.source_key,
                    query_input=query,
                    cas_number=query,
                    substance_name=query,
                    endpoint="",
                    field_name="hazard_code",
                    raw_value=code,
                    comparator="",
                    numeric_value="",
                    unit="",
                    qualifier="",
                    hazard_code=code,
                    hazard_category="",
                    study_guideline="",
                    test_conditions="",
                    section_path="hcis.search",
                    evidence_url=final_url,
                    evidence_file=str(out_file),
                    retrieved_at_utc=self.now_utc_iso(),
                )
            )

        if not rows:
            rows.append(
                UnifiedRow(
                    source_name=self.source_key,
                    query_input=query,
                    cas_number=query,
                    substance_name=query,
                    endpoint="",
                    field_name="search_result_text",
                    raw_value=" ".join(text.split()[:40]),
                    comparator="",
                    numeric_value="",
                    unit="",
                    qualifier="",
                    hazard_code="",
                    hazard_category="",
                    study_guideline="",
                    test_conditions="",
                    section_path="hcis.search.fallback",
                    evidence_url=final_url,
                    evidence_file=str(out_file),
                    retrieved_at_utc=self.now_utc_iso(),
                )
            )

        return rows
