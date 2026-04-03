import json

from source_adapters.ipcs_adapter import IPCSAdapter
from source_adapters.base import RunContext
from pathlib import Path


def test_extract_links_absolute_and_relative():
    html = '''
    <ul>
      <li><a href="/documents/pims/chemical/abc.htm">Acrylamide (PIM 652)</a></li>
      <li><a href="https://www.inchem.org/documents/ehc/ehc/ehc001.htm">EHC 1</a></li>
      <li><a href="#top">Top</a></li>
    </ul>
    '''
    links = IPCSAdapter._extract_links(html, "https://www.inchem.org/pages/pims.html")
    urls = [u for _, u in links]
    assert "https://www.inchem.org/documents/pims/chemical/abc.htm" in urls
    assert "https://www.inchem.org/documents/ehc/ehc/ehc001.htm" in urls
    assert all(not u.endswith("#top") for u in urls)


def test_all_query_aliases():
    assert IPCSAdapter._is_all_query("all")
    assert IPCSAdapter._is_all_query("*")
    assert IPCSAdapter._is_all_query("IPCS_ALL")
    assert not IPCSAdapter._is_all_query("benzene")


def test_extract_hazard_sentences_filters_relevant_content():
    text = """
    This section contains background history.
    Central nervous system depression and coma are reported after poisoning exposure.
    Packaging and storage conditions are listed in this chapter.
    The chemical may cause liver toxicity and respiratory irritation in severe cases.
    """
    hits = IPCSAdapter._extract_hazard_sentences(text)
    joined = "\n".join(hits).lower()
    assert "nervous system depression" in joined
    assert "liver toxicity" in joined
    assert "packaging and storage" not in joined


def test_substance_name_from_label_removes_monograph_tokens():
    text = "Poisons Information Monograph 327 Clorazepate dipotassium"
    name = IPCSAdapter._substance_from_label("Clorazepate dipotassium (PIM 327)", text)
    assert name.lower() == "clorazepate dipotassium"


def test_document_and_listing_url_classification():
    assert IPCSAdapter._is_document_url("https://www.inchem.org/documents/pims/chemical/abc.htm")
    assert IPCSAdapter._is_listing_url("https://www.inchem.org/pages/pims.html")
    assert not IPCSAdapter._is_document_url("https://example.com/documents/x.htm")


def test_collect_all_links_crawls_sub_listing(monkeypatch, tmp_path: Path):
    adapter = IPCSAdapter()
    pages = {
        "https://www.inchem.org/pages/ehc.html": '<a href="/pages/sublist.html">sub</a>',
        "https://www.inchem.org/pages/pims.html": "",
        "https://www.inchem.org/pages/jmpr.html": "",
        "https://www.inchem.org/pages/jecfa.html": "",
        "https://www.inchem.org/pages/sublist.html": '<a href="/documents/pims/chemical/abc.htm">A (PIM 1)</a>',
    }

    def fake_fetch(url: str, timeout: float):
        return pages.get(url, ""), url

    monkeypatch.setattr(adapter, "_fetch", fake_fetch)
    links = adapter._collect_all_links(RunContext(evidence_dir=tmp_path))
    assert any("/documents/pims/chemical/abc.htm" in u for _, _, u in links)


def test_extract_table_chunks_reads_rows_with_cells():
    html = """
    <table>
      <tr><th>Endpoint</th><th>Value</th></tr>
      <tr><td>LD50 (oral, rat)</td><td>240 mg/kg</td></tr>
      <tr><td>Note</td><td>Severe CNS depression</td></tr>
    </table>
    """
    chunks = IPCSAdapter._extract_table_chunks(html)
    assert any("LD50" in c and "240 mg/kg" in c for c in chunks)
    assert any("CNS depression" in c for c in chunks)


def test_collect_writes_manifest_with_content_hash(monkeypatch, tmp_path: Path):
    adapter = IPCSAdapter()

    monkeypatch.setattr(
        adapter,
        "_collect_all_links",
        lambda ctx: [("https://www.inchem.org/pages/pims.html", "Benzene (PIM 001)", "https://www.inchem.org/doc.htm")],
    )

    def fake_fetch_with_meta(url: str, timeout: float):
        html = "<html><body>LD50 50 mg/kg toxic effect</body></html>"
        return html, url, {"url": url, "etag": "abc", "last_modified": "Mon", "content_sha256": IPCSAdapter._content_sha256(html)}

    monkeypatch.setattr(adapter, "_fetch_with_meta", fake_fetch_with_meta)

    ctx = RunContext(evidence_dir=tmp_path)
    rows = adapter.collect("all", ctx)
    assert rows

    manifest_path = tmp_path / "ipcs" / "all" / "manifest.json"
    assert manifest_path.exists()
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload and payload[0]["content_sha256"]


def test_collect_top_per_index_50_each(monkeypatch, tmp_path: Path):
    adapter = IPCSAdapter()

    def fake_fetch(url: str, timeout: float):
        # 각 인덱스에 60개 문서 링크 제공(상단 50개만 선택되어야 함)
        links = []
        for i in range(1, 61):
            links.append(f'<a href="/documents/mock/{i:03d}.htm">Substance {i:03d}</a>')
        return "\n".join(links), url

    fetch_count = {"n": 0}

    def fake_fetch_with_meta(url: str, timeout: float):
        fetch_count["n"] += 1
        html = "<html><body>LD50 100 mg/kg toxic</body></html>"
        return html, url, {"url": url, "etag": "", "last_modified": "", "content_sha256": IPCSAdapter._content_sha256(html)}

    monkeypatch.setattr(adapter, "_fetch", fake_fetch)
    monkeypatch.setattr(adapter, "_fetch_with_meta", fake_fetch_with_meta)

    rows = adapter.collect_top_per_index(50, RunContext(evidence_dir=tmp_path))
    assert rows
    # 4개 인덱스 × 50개 문서
    assert fetch_count["n"] == 200
