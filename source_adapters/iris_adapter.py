from __future__ import annotations

import re
import time
from urllib.parse import urlencode
from urllib.request import ProxyHandler, Request, build_opener, urlopen

from .base import BaseAdapter, RunContext, UnifiedRow
from .utils import split_measurement


class IRISAdapter(BaseAdapter):
    source_key = "iris"

    @staticmethod
    def _fetch(url: str, params: dict | None, timeout: float, proxy: str = ""):
        if params:
            query = urlencode(params)
            full_url = f"{url}?{query}"
        else:
            full_url = url
        req = Request(full_url, headers={"User-Agent": "Mozilla/5.0"})
        if proxy:
            opener = build_opener(ProxyHandler({"http": proxy, "https": proxy}))
            resp_obj = opener.open(req, timeout=timeout)
        else:
            resp_obj = urlopen(req, timeout=timeout)
        with resp_obj as resp:
            status = getattr(resp, "status", 200)
            body = resp.read().decode("utf-8", errors="ignore")
            final_url = resp.geturl()
        return status, body, final_url

    def _fetch_with_retry(self, url: str, params: dict | None, ctx: RunContext):
        last_exc: Exception | None = None
        for i in range(max(1, ctx.retries + 1)):
            try:
                return self._fetch(url, params, ctx.timeout_sec, ctx.proxy)
            except Exception as exc:
                last_exc = exc
                if i < ctx.retries:
                    time.sleep(ctx.backoff_sec * (i + 1))
        assert last_exc is not None
        raise last_exc

    def collect(self, query: str, ctx: RunContext) -> list[UnifiedRow]:
        candidates: list[tuple[str, dict | None]] = [
            ("https://www.epa.gov/iris/search", {"search_api_fulltext": query}),
            ("https://www.epa.gov/search", {"query": f"{query} iris"}),
            ("https://www.epa.gov/iris", None),
        ]

        html = ""
        final_url = ""
        last_exc: Exception | None = None
        for url, params in candidates:
            try:
                status, body, got_url = self._fetch_with_retry(url, params, ctx)
                # 404는 다음 후보로 진행
                if status == 404:
                    continue
                if status >= 400:
                    continue
                html = body
                final_url = got_url
                break
            except Exception as exc:
                last_exc = exc
                continue

        if not html:
            if last_exc:
                raise last_exc
            raise RuntimeError("IRIS 접근 실패")

        out_file = ctx.evidence_dir / f"{self.source_key}_{query}.html"
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text(html, encoding="utf-8")

        def html_to_text(x: str) -> str:
            return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "\n", x)).strip()

        text = html_to_text(html)
        rows: list[UnifiedRow] = []

        # 검색 결과에서 IRIS 상세 링크 추출
        detail_links = sorted(set(re.findall(r'href=[\"\'](https://www\\.epa\\.gov/iris/[^\"\']+)[\"\']', html)))
        detail_pages = [(final_url, html)]  # 검색 페이지도 파싱
        for link in detail_links[:3]:
            try:
                st, body, got = self._fetch_with_retry(link, None, ctx)
                if st < 400:
                    detail_pages.append((got, body))
            except Exception:
                continue

        patterns = [
            ("Reference dose", r"Reference dose[^\n]{0,120}"),
            ("Reference concentration", r"Reference concentration[^\n]{0,120}"),
            ("Cancer", r"Cancer[^\n]{0,120}"),
            ("Oral slope factor", r"Oral slope factor[^\n]{0,120}"),
            ("IRIS Search Hit", rf"{re.escape(query)}[^\n]{{0,120}}"),
        ]
        for page_url, page_html in detail_pages:
            page_text = html_to_text(page_html)
            for endpoint, pat in patterns:
                for m in re.finditer(pat, page_text, re.I):
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
                            evidence_url=page_url,
                            evidence_file=str(out_file),
                            retrieved_at_utc=self.now_utc_iso(),
                        )
                    )

        if not rows:
            # 최소 1행은 남겨 근거 페이지 도달 여부를 확인 가능하게 함
            snippet = " ".join(text.split()[:40])
            rows.append(
                UnifiedRow(
                    source_name=self.source_key,
                    query_input=query,
                    cas_number=query,
                    substance_name=query,
                    endpoint="",
                    field_name="search_result_text",
                    raw_value=snippet,
                    comparator="",
                    numeric_value="",
                    unit="",
                    qualifier="",
                    hazard_code="",
                    hazard_category="",
                    study_guideline="",
                    test_conditions="",
                    section_path="iris.search.fallback",
                    evidence_url=final_url,
                    evidence_file=str(out_file),
                    retrieved_at_utc=self.now_utc_iso(),
                )
            )

        return rows
