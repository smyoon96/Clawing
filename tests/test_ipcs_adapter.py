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
