from __future__ import annotations

import re
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .base import BaseAdapter, RunContext, UnifiedRow
from .utils import extract_hazard_codes, split_measurement


class IPCSAdapter(BaseAdapter):
    source_key = "ipcs"

    @staticmethod
    def _fetch(url: str, params: dict | None, timeout: float):
        full_url = f"{url}?{urlencode(params)}" if params else url
        req = Request(full_url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            final_url = resp.geturl()
        return body, final_url

    def collect(self, query: str, ctx: RunContext) -> list[UnifiedRow]:
        # IPCS/INCHEM 계열 페이지 후보
        candidates = [
            ("https://inchem.org/pages/ehc.html", None),
            ("https://inchem.org/pages/pims.html", None),
            ("https://inchem.org/pages/jmpr.html", None),
            ("https://inchem.org/pages/jecfa.html", None),
            ("https://inchem.org/cgi-bin/full_doc.pl", {"search": query}),
        ]

        html = ""
        final_url = ""
        for url, params in candidates:
            try:
                body, got = self._fetch(url, params, ctx.timeout_sec)
                html = body
                final_url = got
                if query.lower() in body.lower():
                    break
            except Exception:
                continue

        if not html:
            raise RuntimeError("IPCS 접근 실패")

        out_file = ctx.evidence_dir / f"{self.source_key}_{query}.html"
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text(html, encoding="utf-8")

        text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html)).strip()
        rows: list[UnifiedRow] = []

        # 키워드 기반 endpoint 힌트
        keyword_patterns = [
            ("EHC", r"EHC\s*\d+[^\.\n]{0,120}"),
            ("PIM", r"PIM\s*\d+[^\.\n]{0,120}"),
            ("JMPR", r"JMPR[^\.\n]{0,120}"),
            ("JECFA", r"JECFA[^\.\n]{0,120}"),
        ]
        for endpoint, pat in keyword_patterns:
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
                        field_name="ipcs_reference",
                        raw_value=chunk,
                        comparator=cmp_,
                        numeric_value=num,
                        unit=unit,
                        qualifier=qual,
                        hazard_code="",
                        hazard_category="",
                        study_guideline="",
                        test_conditions="",
                        section_path="ipcs.search",
                        evidence_url=final_url,
                        evidence_file=str(out_file),
                        retrieved_at_utc=self.now_utc_iso(),
                    )
                )

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
                    section_path="ipcs.search",
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
                    section_path="ipcs.search.fallback",
                    evidence_url=final_url,
                    evidence_file=str(out_file),
                    retrieved_at_utc=self.now_utc_iso(),
                )
            )

        return rows
