from source_adapters.ipcs_adapter import IPCSAdapter


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
