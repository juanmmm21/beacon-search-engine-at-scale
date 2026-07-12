"""Tests de `extract_single_page` -- mismos casos que `html-content-extractor` cubre en
`tests/test_pipeline.py` para `ExtractionPipeline._process_record`, ya que
`extract_single_page` reimplementa exactamente ese mismo orden de etapas para una única
página (ver `page_extractor.py`, docstring del módulo)."""

from __future__ import annotations

from html_content_extractor.models import (
    DiscardedPage,
    DiscardReason,
    ExtractedDocument,
    ExtractionConfig,
)

from beacon_scale_infra.extract.page_extractor import extract_single_page

_LONG_ARTICLE_HTML = """
<html lang="en"><head><title>Long Article</title>
<meta property="og:title" content="A Long Article About Search"></head>
<body>
<header><nav><a href="/">Home</a></nav></header>
<article class="post-content">
<h1>A Long Article About Search</h1>
<p>This paragraph contains a substantial amount of real prose about how search engines
work, why inverted indexes matter, and how relevance ranking combines multiple signals
together to produce a useful ordering of results for the end user.</p>
<p>This second paragraph continues with more depth on tokenization, tolerant HTML
parsing, and the general engineering trade-offs involved in building a search engine
completely from scratch without leaning on any third-party indexing library.</p>
</article>
<footer><p>Copyright Example Corp.</p></footer>
</body></html>
"""


def _base_record(**overrides: object) -> dict[str, object]:
    record: dict[str, object] = {
        "url": "http://example.com/article",
        "final_url": "http://example.com/article",
        "status_code": 200,
        "headers": {"Content-Type": "text/html; charset=utf-8"},
        "html": _LONG_ARTICLE_HTML,
        "fetched_at": "2024-01-15T09:00:00+00:00",
        "depth": 1,
        "content_type": "text/html",
    }
    record.update(overrides)
    return record


def test_extracts_a_valid_article() -> None:
    result = extract_single_page(_base_record(), ExtractionConfig())

    assert isinstance(result, ExtractedDocument)
    assert result.title == "A Long Article About Search"
    assert "substantial amount of real prose" in result.main_text
    assert "Copyright Example Corp" not in result.main_text
    assert result.language == "en"
    assert result.depth == 1
    assert result.word_count > 0


def test_discards_non_html_content_type_without_parsing() -> None:
    record = _base_record(headers={"Content-Type": "application/pdf"}, html="%PDF-1.4 ...")

    result = extract_single_page(record, ExtractionConfig())

    assert isinstance(result, DiscardedPage)
    assert result.reason == DiscardReason.NON_HTML_CONTENT
    assert result.url == "http://example.com/article"


def test_discards_page_with_insufficient_main_content() -> None:
    record = _base_record(html="<html><body><p>Too short.</p></body></html>")

    result = extract_single_page(record, ExtractionConfig())

    assert isinstance(result, DiscardedPage)
    assert result.reason == DiscardReason.EMPTY_MAIN_CONTENT


def test_discards_pathologically_nested_html_without_raising() -> None:
    deeply_broken = "<div>" * 5000 + "<p>unreachable</p>"
    record = _base_record(html=deeply_broken)

    result = extract_single_page(record, ExtractionConfig())

    assert isinstance(result, DiscardedPage)
    assert result.reason == DiscardReason.PARSE_ERROR


def test_recovers_mojibake_and_records_encoding_used() -> None:
    mojibake_html = (
        _LONG_ARTICLE_HTML.replace("search engines", "café en España — search engines")
        .encode("utf-8")
        .decode("iso-8859-1")
    )
    record = _base_record(
        html=mojibake_html, headers={"Content-Type": "text/html; charset=iso-8859-1"}
    )

    result = extract_single_page(record, ExtractionConfig())

    assert isinstance(result, ExtractedDocument)
    assert "café en España" in result.main_text
    assert result.encoding_used == "utf-8"


def test_honors_custom_extraction_config_threshold() -> None:
    short_record = _base_record(
        html="<html><body><article><p>"
        + "A moderately short but legitimate paragraph of prose. " * 3
        + "</p></article></body></html>"
    )

    result_default = extract_single_page(short_record, ExtractionConfig())
    result_lenient = extract_single_page(short_record, ExtractionConfig(min_main_content_chars=50))

    assert isinstance(result_default, DiscardedPage)
    assert isinstance(result_lenient, ExtractedDocument)
