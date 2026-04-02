from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from .base import BaseAdapter, RunContext, UnifiedRow
from .utils import extract_hazard_codes, split_measurement


class IPCSAdapter(BaseAdapter):
    source_key = "ipcs"

    INDEX_URLS: tuple[str, ...] = (
        "https://www.inchem.org/pages/ehc.html",
        "https://www.inchem.org/pages/pims.html",
        "https://www.inchem.org/pages/jmpr.html",
        "https://www.inchem.org/pages/jecfa.html",
    )

    @staticmethod
    def _fetch(url: str, timeout: float) -> tuple[str, str]:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            final_url = resp.geturl()
        return body, final_url

    @staticmethod
    def _text_only(html: str) -> str:
        return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html)).strip()

    @staticmethod
    def _safe_token(value: str) -> str:
        token = re.sub(r"[^a-zA-Z0-9._-]+", "_", value.strip())
        return token.strip("_")[:120] or "doc"

    @staticmethod
    def _extract_links(index_html: str, base_url: str) -> list[tuple[str, str]]:
        out: list[tuple[str, str]] = []
        seen: set[str] = set()

        # <a href="...">text</a>
        for m in re.finditer(
            r"<a\b[^>]*href=[\"'](?P<href>[^\"']+)[\"'][^>]*>(?P<label>.*?)</a>",
            index_html,
            flags=re.I | re.S,
        ):
            href = (m.group("href") or "").strip()
            label = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", m.group("label") or "")).strip()
            if not href or href.startswith("#"):
                continue
            full = urljoin(base_url, href)
            if "inchem.org" not in full.lower():
                continue
            if full in seen:
                continue
            seen.add(full)
            out.append((label, full))

        return out

    @staticmethod
    def _is_all_query(query: str) -> bool:
        return query.strip().lower() in {"*", "all", "__all__", "ipcs_all"}

    @staticmethod
    def _match_link(query: str, label: str, url: str) -> bool:
        q = query.strip().lower()
        if not q:
            return False
        if q in label.lower() or q in url.lower():
            return True

        q_cas_match = re.search(r"\b\d{2,7}-\d{2}-\d\b", q)
        if q_cas_match and q_cas_match.group(0) in f"{label} {url}":
            return True
        return False

    def _build_doc_rows(
        self,
        *,
        query: str,
        index_url: str,
        index_label: str,
        doc_url: str,
        doc_html: str,
        evidence_file: Path,
    ) -> list[UnifiedRow]:
        doc_text = self._text_only(doc_html)
        rows: list[UnifiedRow] = []

        # 문서 제목 / 레퍼런스 row
        rows.append(
            UnifiedRow(
                source_name=self.source_key,
                query_input=query,
                cas_number=query,
                substance_name=query,
                endpoint=index_label,
                field_name="ipcs_document",
                raw_value=index_label,
                comparator="",
                numeric_value="",
                unit="",
                qualifier="",
                hazard_code="",
                hazard_category="",
                study_guideline="",
                test_conditions="",
                section_path=f"ipcs.index.{self._safe_token(index_url)}",
                evidence_url=doc_url,
                evidence_file=str(evidence_file),
                retrieved_at_utc=self.now_utc_iso(),
            )
        )

        # CAS 번호
        for cas in sorted(set(re.findall(r"\b\d{2,7}-\d{2}-\d\b", doc_text))):
            rows.append(
                UnifiedRow(
                    source_name=self.source_key,
                    query_input=query,
                    cas_number=cas,
                    substance_name=query,
                    endpoint=index_label,
                    field_name="cas_number_detected",
                    raw_value=cas,
                    comparator="",
                    numeric_value="",
                    unit="",
                    qualifier="",
                    hazard_code="",
                    hazard_category="",
                    study_guideline="",
                    test_conditions="",
                    section_path="ipcs.document.identifiers",
                    evidence_url=doc_url,
                    evidence_file=str(evidence_file),
                    retrieved_at_utc=self.now_utc_iso(),
                )
            )

        # H-code
        for code in extract_hazard_codes(doc_text):
            rows.append(
                UnifiedRow(
                    source_name=self.source_key,
                    query_input=query,
                    cas_number=query,
                    substance_name=query,
                    endpoint=index_label,
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
                    section_path="ipcs.document.hazard",
                    evidence_url=doc_url,
                    evidence_file=str(evidence_file),
                    retrieved_at_utc=self.now_utc_iso(),
                )
            )

        # 수치 힌트
        for endpoint, pat in [
            ("EHC", r"EHC\s*\d+[^\.\n]{0,120}"),
            ("PIM", r"PIM\s*\d+[^\.\n]{0,120}"),
            ("JMPR", r"JMPR[^\.\n]{0,120}"),
            ("JECFA", r"JECFA[^\.\n]{0,120}"),
            ("TOX", r"(?:LD50|LC50|NOAEL|LOAEL)[^\.\n]{0,120}"),
        ]:
            for m in re.finditer(pat, doc_text, flags=re.I):
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
                        section_path="ipcs.document.extract",
                        evidence_url=doc_url,
                        evidence_file=str(evidence_file),
                        retrieved_at_utc=self.now_utc_iso(),
                    )
                )

        if len(rows) == 1:
            rows.append(
                UnifiedRow(
                    source_name=self.source_key,
                    query_input=query,
                    cas_number=query,
                    substance_name=query,
                    endpoint=index_label,
                    field_name="search_result_text",
                    raw_value=" ".join(doc_text.split()[:60]),
                    comparator="",
                    numeric_value="",
                    unit="",
                    qualifier="",
                    hazard_code="",
                    hazard_category="",
                    study_guideline="",
                    test_conditions="",
                    section_path="ipcs.document.fallback",
                    evidence_url=doc_url,
                    evidence_file=str(evidence_file),
                    retrieved_at_utc=self.now_utc_iso(),
                )
            )

        return rows

    def collect(self, query: str, ctx: RunContext) -> list[UnifiedRow]:
        fetch_all = self._is_all_query(query)
        evidence_root = ctx.evidence_dir / self.source_key / self._safe_token(query)
        evidence_root.mkdir(parents=True, exist_ok=True)

        index_cache: list[tuple[str, str, str]] = []  # (index_url, html, final_url)
        all_links: list[tuple[str, str, str]] = []  # (index_url, label, doc_url)

        for index_url in self.INDEX_URLS:
            try:
                html, final_url = self._fetch(index_url, ctx.timeout_sec)
            except Exception:
                continue

            index_cache.append((index_url, html, final_url))
            for label, doc_url in self._extract_links(html, final_url):
                all_links.append((index_url, label, doc_url))

        if not all_links:
            raise RuntimeError("IPCS 인덱스에서 문서 링크를 찾지 못했습니다")

        selected_links: list[tuple[str, str, str]] = []
        seen_doc: set[str] = set()
        for index_url, label, doc_url in all_links:
            if doc_url in seen_doc:
                continue
            if fetch_all or self._match_link(query, label, doc_url):
                seen_doc.add(doc_url)
                selected_links.append((index_url, label, doc_url))

        if not selected_links and index_cache:
            # 링크 매칭 실패 시 인덱스 문서 텍스트라도 반환
            rows: list[UnifiedRow] = []
            for index_url, html, final_url in index_cache:
                out_file = evidence_root / f"index_{self._safe_token(index_url)}.html"
                out_file.write_text(html, encoding="utf-8")
                rows.append(
                    UnifiedRow(
                        source_name=self.source_key,
                        query_input=query,
                        cas_number=query,
                        substance_name=query,
                        endpoint="index",
                        field_name="search_result_text",
                        raw_value=" ".join(self._text_only(html).split()[:80]),
                        comparator="",
                        numeric_value="",
                        unit="",
                        qualifier="",
                        hazard_code="",
                        hazard_category="",
                        study_guideline="",
                        test_conditions="",
                        section_path=f"ipcs.index.{self._safe_token(index_url)}.fallback",
                        evidence_url=final_url,
                        evidence_file=str(out_file),
                        retrieved_at_utc=self.now_utc_iso(),
                    )
                )
            return rows

        rows: list[UnifiedRow] = []
        for i, (index_url, label, doc_url) in enumerate(selected_links, start=1):
            try:
                doc_html, final_doc_url = self._fetch(doc_url, ctx.timeout_sec)
            except Exception:
                continue

            out_file = evidence_root / f"doc_{i:04d}_{self._safe_token(label) or 'untitled'}.html"
            out_file.write_text(doc_html, encoding="utf-8")
            rows.extend(
                self._build_doc_rows(
                    query=query,
                    index_url=index_url,
                    index_label=label or "untitled",
                    doc_url=final_doc_url,
                    doc_html=doc_html,
                    evidence_file=out_file,
                )
            )

        if not rows:
            raise RuntimeError("IPCS 문서 본문 수집 실패")

        return rows
