from __future__ import annotations

import hashlib
import json
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

    HAZARD_KEYWORDS: tuple[str, ...] = (
        "tox",
        "toxic",
        "poison",
        "fatal",
        "carcin",
        "mutagen",
        "repro",
        "irrit",
        "sensiti",
        "corros",
        "neuro",
        "hepat",
        "renal",
        "respirat",
        "cns",
        "target organ",
        "risk",
        "hazard",
    )

    EHC_SECTION_CATEGORY: dict[str, str] = {
        "1.1": "physchem_properties",
        "1.2": "exposure_sources",
        "1.3": "environmental_fate",
        "1.4": "environmental_levels_human_exposure",
        "1.5": "toxicokinetics_metabolism",
        "1.6": "human_health_hazard_animals",
        "1.7": "human_health_hazard_humans",
        "1.8": "ecotoxicity",
    }

    @staticmethod
    def _fetch(url: str, timeout: float) -> tuple[str, str]:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            final_url = resp.geturl()
        return body, final_url

    @staticmethod
    def _content_sha256(text: str) -> str:
        return hashlib.sha256((text or "").encode("utf-8", errors="ignore")).hexdigest()

    @staticmethod
    def _fetch_with_meta(url: str, timeout: float) -> tuple[str, str, dict[str, str]]:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            final_url = resp.geturl()
            headers = resp.headers

        meta = {
            "url": final_url,
            "etag": headers.get("ETag", ""),
            "last_modified": headers.get("Last-Modified", ""),
            "content_sha256": IPCSAdapter._content_sha256(body),
        }
        return body, final_url, meta

    @staticmethod
    def _text_only(html: str) -> str:
        return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html)).strip()

    @staticmethod
    def _text_with_breaks(html: str) -> str:
        marked = re.sub(r"<(?:br|/p|/div|/li|/tr|/h[1-6])\b[^>]*>", "\n", html or "", flags=re.I)
        no_tags = re.sub(r"<[^>]+>", " ", marked)
        no_tags = re.sub(r"[ \t\r\f\v]+", " ", no_tags)
        no_tags = re.sub(r"\n+", "\n", no_tags)
        return no_tags.strip()

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
    def _is_document_url(url: str) -> bool:
        u = (url or "").lower()
        if "inchem.org" not in u:
            return False
        if any(x in u for x in ["/documents/", "/monographs/"]):
            return u.endswith((".htm", ".html", ".pdf")) or "/documents/" in u
        return False

    @staticmethod
    def _is_listing_url(url: str) -> bool:
        u = (url or "").lower()
        if "inchem.org" not in u:
            return False
        if "/pages/" in u:
            return True
        return u.endswith(("index.htm", "index.html"))

    def _collect_all_links(self, ctx: RunContext) -> list[tuple[str, str, str]]:
        queue = list(self.INDEX_URLS)
        visited: set[str] = set()
        doc_links: list[tuple[str, str, str]] = []
        seen_docs: set[str] = set()

        # 인덱스/서브인덱스를 순회하면서 문서 링크를 전수 수집
        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            try:
                html, final_url = self._fetch(current, ctx.timeout_sec)
            except Exception:
                continue

            for label, link in self._extract_links(html, final_url):
                if self._is_document_url(link):
                    if link not in seen_docs:
                        seen_docs.add(link)
                        doc_links.append((current, label, link))
                    continue
                if self._is_listing_url(link) and link not in visited and link not in queue:
                    queue.append(link)

        return doc_links

    def _collect_top_links_per_index(self, ctx: RunContext, per_index: int) -> list[tuple[str, str, str]]:
        selected: list[tuple[str, str, str]] = []
        for index_url in self.INDEX_URLS:
            try:
                html, final_url = self._fetch(index_url, ctx.timeout_sec)
            except Exception:
                continue

            count = 0
            for label, link in self._extract_links(html, final_url):
                if not self._is_document_url(link):
                    continue
                selected.append((index_url, label, link))
                count += 1
                if count >= per_index:
                    break
        return selected

    def _collect_rows_from_links(
        self,
        *,
        query: str,
        selected_links: list[tuple[str, str, str]],
        evidence_root: Path,
        timeout_sec: float,
    ) -> list[UnifiedRow]:
        rows: list[UnifiedRow] = []
        manifest: list[dict[str, str]] = []
        for i, (index_url, label, doc_url) in enumerate(selected_links, start=1):
            try:
                doc_html, final_doc_url, meta = self._fetch_with_meta(doc_url, timeout_sec)
            except Exception:
                continue

            out_file = evidence_root / f"doc_{i:04d}_{self._safe_token(label) or 'untitled'}.html"
            out_file.write_text(doc_html, encoding="utf-8")
            doc_rows = self._build_doc_rows(
                query=query,
                index_url=index_url,
                index_label=label or "untitled",
                doc_url=final_doc_url,
                doc_html=doc_html,
                evidence_file=out_file,
            )

            cas_values = sorted({r.raw_value for r in doc_rows if r.field_name == "cas_number_detected"})
            hazard_codes = sorted({r.hazard_code or r.raw_value for r in doc_rows if r.field_name == "hazard_code"})
            tox_refs = [
                r.raw_value
                for r in doc_rows
                if r.field_name in {"ipcs_reference", "table_hazard_extract", "toxicity_metric", "ehc_endpoint_measurement"}
            ][:5]
            summary_rows = [r.raw_value for r in doc_rows if r.field_name == "hazard_summary"]
            substance = next((r.substance_name for r in doc_rows if r.substance_name), self._substance_from_label(label, doc_html))

            manifest.append(
                {
                    "index_url": index_url,
                    "label": label,
                    "doc_url": final_doc_url,
                    "evidence_file": str(out_file),
                    "substance_name": substance,
                    "detected_cas": "; ".join(cas_values),
                    "hazard_codes": "; ".join(hazard_codes),
                    "toxicity_refs_preview": " || ".join(tox_refs),
                    "hazard_summary_preview": (summary_rows[0] if summary_rows else ""),
                    "extracted_row_count": str(len(doc_rows)),
                    **meta,
                }
            )
            rows.extend(doc_rows)

        (evidence_root / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        return rows

    def collect_top_per_index(self, per_index: int, ctx: RunContext) -> list[UnifiedRow]:
        evidence_root = ctx.evidence_dir / self.source_key / f"top_{per_index}"
        evidence_root.mkdir(parents=True, exist_ok=True)

        selected_links = self._collect_top_links_per_index(ctx, per_index)
        if not selected_links:
            raise RuntimeError("IPCS top-per-index 링크를 찾지 못했습니다")

        rows = self._collect_rows_from_links(
            query=f"top{per_index}",
            selected_links=selected_links,
            evidence_root=evidence_root,
            timeout_sec=ctx.timeout_sec,
        )
        if not rows:
            raise RuntimeError("IPCS top-per-index 본문 수집 실패")
        return rows

    @classmethod
    def _extract_hazard_sentences(cls, doc_text: str) -> list[str]:
        candidates = re.split(r"(?<=[.!?])\s+|\n+", doc_text)
        out: list[str] = []
        seen: set[str] = set()
        for sent in candidates:
            s = " ".join(sent.split()).strip()
            if len(s) < 30:
                continue
            low = s.lower()
            if any(k in low for k in cls.HAZARD_KEYWORDS):
                key = s[:220]
                if key in seen:
                    continue
                seen.add(key)
                out.append(key)
        return out

    @staticmethod
    def _extract_table_chunks(doc_html: str) -> list[str]:
        chunks: list[str] = []
        for tr in re.finditer(r"<tr\b[^>]*>(.*?)</tr>", doc_html or "", flags=re.I | re.S):
            row_html = tr.group(1)
            cells = re.findall(r"<(?:td|th)\b[^>]*>(.*?)</(?:td|th)>", row_html, flags=re.I | re.S)
            texts = [re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", c)).strip() for c in cells]
            texts = [t for t in texts if t]
            if len(texts) >= 2:
                chunks.append(" | ".join(texts)[:300])
        return chunks

    @classmethod
    def _extract_ehc_endpoint_chunks(cls, doc_text_with_breaks: str) -> list[tuple[str, str, str]]:
        out: list[tuple[str, str, str]] = []
        matches = list(
            re.finditer(
                r"(?P<section>1\.[1-8])\s+(?P<title>[^\n]+)",
                doc_text_with_breaks or "",
                flags=re.I,
            )
        )
        if not matches:
            return out

        for idx, m in enumerate(matches):
            sec = m.group("section")
            start = m.end()
            end = matches[idx + 1].start() if idx + 1 < len(matches) else len(doc_text_with_breaks)
            body = " ".join(doc_text_with_breaks[start:end].split())
            if not body:
                continue
            category = cls.EHC_SECTION_CATEGORY.get(sec, "ehc_summary")
            out.append((sec, category, body[:2000]))
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

    @staticmethod
    def _substance_from_label(label: str, doc_text: str) -> str:
        cleaned = re.sub(r"\([^)]*\)", "", label or "")
        cleaned = re.sub(r"\b(?:PIM|EHC|JMPR|JECFA)\s*\d*\b", "", cleaned, flags=re.I)
        cleaned = re.sub(r"^\s*\d+[\.)]?\s*", "", cleaned)
        cleaned = " ".join(cleaned.split("-")[0].split()).strip(" -:;")
        if cleaned:
            return cleaned

        # fallback: 문서 선두 텍스트에서 물질명 유추
        head = " ".join(doc_text.split()[:30])
        m = re.search(r"\b([A-Z][A-Za-z0-9\- ]{2,80})\b", head)
        return (m.group(1).strip() if m else "") or "unknown_substance"

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
        doc_text_breaks = self._text_with_breaks(doc_html)
        substance_name = self._substance_from_label(index_label, doc_text)
        rows: list[UnifiedRow] = []

        # CAS 번호
        for cas in sorted(set(re.findall(r"\b\d{2,7}-\d{2}-\d\b", doc_text))):
            rows.append(
                UnifiedRow(
                    source_name=self.source_key,
                    query_input=query,
                    cas_number=cas,
                    substance_name=substance_name,
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
                    cas_number="",
                    substance_name=substance_name,
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

        # 수치 기반 유해성 지표
        tox_pat = (
            r"(?:LD50|LC50|EC50|IC50|NOAEL|NOEL|LOAEL|LOEL|ADI|TDI|BMDL|BMD)"
            r"[^\.\n]{0,180}"
        )
        for endpoint, pat in [
            ("EHC", r"EHC\s*\d+[^\.\n]{0,120}"),
            ("PIM", r"PIM\s*\d+[^\.\n]{0,120}"),
            ("JMPR", r"JMPR[^\.\n]{0,120}"),
            ("JECFA", r"JECFA[^\.\n]{0,120}"),
            ("TOX", tox_pat),
        ]:
            for m in re.finditer(pat, doc_text, flags=re.I):
                chunk = m.group(0)
                cmp_, num, unit, qual = split_measurement(chunk)
                rows.append(
                    UnifiedRow(
                        source_name=self.source_key,
                        query_input=query,
                        cas_number="",
                        substance_name=substance_name,
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

        # TOX 키워드 없이 단위가 나오는 경우 보강 추출(예: "> 10 mg/litre")
        for m in re.finditer(
            r"(?:acute|chronic|dietary|oral|dermal|inhalation)[^\.\n]{0,120}(?:<=|>=|<|>|=)?\s*\d+(?:[.,]\d+)?\s*(?:mg/kg|mg/litre|g/litre|ppm)",
            doc_text,
            flags=re.I,
        ):
            chunk = m.group(0)
            cmp_, num, unit, qual = split_measurement(chunk)
            rows.append(
                UnifiedRow(
                    source_name=self.source_key,
                    query_input=query,
                    cas_number="",
                    substance_name=substance_name,
                    endpoint="TOX",
                    field_name="toxicity_metric",
                    raw_value=chunk,
                    comparator=cmp_,
                    numeric_value=num,
                    unit=unit,
                    qualifier=qual,
                    hazard_code="",
                    hazard_category="",
                    study_guideline="",
                    test_conditions="",
                    section_path="ipcs.document.metric",
                    evidence_url=doc_url,
                    evidence_file=str(evidence_file),
                    retrieved_at_utc=self.now_utc_iso(),
                )
            )

        # 문장 기반 유해성 요약(키워드 필터)
        for sentence in self._extract_hazard_sentences(doc_text):
            rows.append(
                UnifiedRow(
                    source_name=self.source_key,
                    query_input=query,
                    cas_number="",
                    substance_name=substance_name,
                    endpoint="HAZARD_SUMMARY",
                    field_name="hazard_summary",
                    raw_value=sentence,
                    comparator="",
                    numeric_value="",
                    unit="",
                    qualifier="",
                    hazard_code="",
                    hazard_category="",
                    study_guideline="",
                    test_conditions="",
                    section_path="ipcs.document.summary",
                    evidence_url=doc_url,
                    evidence_file=str(evidence_file),
                    retrieved_at_utc=self.now_utc_iso(),
                )
            )

        # 테이블 기반 유해성 추출
        for chunk in self._extract_table_chunks(doc_html):
            low = chunk.lower()
            if not any(k in low for k in self.HAZARD_KEYWORDS) and not re.search(
                r"\b(?:LD50|LC50|NOAEL|LOAEL|H\d{3}|EUH\d{3})\b", chunk, flags=re.I
            ):
                continue

            cmp_, num, unit, qual = split_measurement(chunk)
            rows.append(
                UnifiedRow(
                    source_name=self.source_key,
                    query_input=query,
                    cas_number="",
                    substance_name=substance_name,
                    endpoint="TABLE_HAZARD",
                    field_name="table_hazard_extract",
                    raw_value=chunk,
                    comparator=cmp_,
                    numeric_value=num,
                    unit=unit,
                    qualifier=qual,
                    hazard_code="",
                    hazard_category="",
                    study_guideline="",
                    test_conditions="",
                    section_path="ipcs.document.table",
                    evidence_url=doc_url,
                    evidence_file=str(evidence_file),
                    retrieved_at_utc=self.now_utc_iso(),
                )
            )

        # EHC summary 섹션(1.1~1.8) 기반 endpoint 추출
        for sec, category, chunk in self._extract_ehc_endpoint_chunks(doc_text_breaks):
            # 수치/단위 패턴 우선 추출
            meas_matches = list(
                re.finditer(
                    r"(?:<=|>=|<|>|=)?\s*\d+(?:[.,]\d+)?\s*(?:mg/kg(?: body weight)?|mg/litre|g/litre|mg/m3|kg/ha|days?|weeks?|h|C)",
                    chunk,
                    flags=re.I,
                )
            )
            if meas_matches:
                for mm in meas_matches[:20]:
                    val = mm.group(0)
                    cmp_, num, unit, qual = split_measurement(val)
                    rows.append(
                        UnifiedRow(
                            source_name=self.source_key,
                            query_input=query,
                            cas_number="",
                            substance_name=substance_name,
                            endpoint=category,
                            field_name="ehc_endpoint_measurement",
                            raw_value=val,
                            comparator=cmp_,
                            numeric_value=num,
                            unit=unit,
                            qualifier=qual,
                            hazard_code="",
                            hazard_category=category,
                            study_guideline="",
                            test_conditions=f"EHC {sec}",
                            section_path=f"ipcs.ehc.{sec}",
                            evidence_url=doc_url,
                            evidence_file=str(evidence_file),
                            retrieved_at_utc=self.now_utc_iso(),
                        )
                    )
            else:
                rows.append(
                    UnifiedRow(
                        source_name=self.source_key,
                        query_input=query,
                        cas_number="",
                        substance_name=substance_name,
                        endpoint=category,
                        field_name="ehc_endpoint_summary",
                        raw_value=chunk[:280],
                        comparator="",
                        numeric_value="",
                        unit="",
                        qualifier="",
                        hazard_code="",
                        hazard_category=category,
                        study_guideline="",
                        test_conditions=f"EHC {sec}",
                        section_path=f"ipcs.ehc.{sec}",
                        evidence_url=doc_url,
                        evidence_file=str(evidence_file),
                        retrieved_at_utc=self.now_utc_iso(),
                    )
                )

        if not rows:
            return []

        # 중복 제거: 동일 필드/원문/근거URL
        deduped: list[UnifiedRow] = []
        seen_keys: set[tuple[str, str, str]] = set()
        for row in rows:
            key = (row.field_name, row.raw_value, row.evidence_url)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            deduped.append(row)

        return deduped

    def collect(self, query: str, ctx: RunContext) -> list[UnifiedRow]:
        fetch_all = self._is_all_query(query)
        evidence_root = ctx.evidence_dir / self.source_key / self._safe_token(query)
        evidence_root.mkdir(parents=True, exist_ok=True)
        if fetch_all:
            selected_links = self._collect_all_links(ctx)
            if not selected_links:
                raise RuntimeError("IPCS 전체 수집 링크를 찾지 못했습니다")

            rows = self._collect_rows_from_links(
                query=query,
                selected_links=selected_links,
                evidence_root=evidence_root,
                timeout_sec=ctx.timeout_sec,
            )

            if not rows:
                raise RuntimeError("IPCS 전체 문서 본문 수집 실패")
            return rows

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

        rows = self._collect_rows_from_links(
            query=query,
            selected_links=selected_links,
            evidence_root=evidence_root,
            timeout_sec=ctx.timeout_sec,
        )

        if not rows:
            raise RuntimeError("IPCS 문서 본문 수집 실패")

        return rows
