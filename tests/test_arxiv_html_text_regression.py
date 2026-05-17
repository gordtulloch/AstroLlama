"""Regression tests for arXiv HTML text extraction limits."""

from __future__ import annotations

import sys

import pytest

sys.path.insert(0, r"c:\Projects\AstroLlama")

from mcp_server.tools.arxiv_search_tool import Tools


class _FakeResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code


def _extract_summary_payload(result: str) -> str:
    marker = "HTML text for summarization:\n"
    assert marker in result
    return result.split(marker, 1)[1]


@pytest.mark.asyncio
async def test_load_paper_html_text_returns_full_text_when_max_chars_omitted(monkeypatch):
    tool = Tools()
    entry = {
        "title": "Test Paper",
        "authors": "A. Author",
        "published": "2026-01-01",
        "category": "astro-ph",
        "id": "2401.00001v1",
    }

    html = """
    <html>
      <body>
        <article>
          <h1>Paper Heading</h1>
          <p>First paragraph before image.</p>
          <img src=\"figure1.png\" />
          <p>Second paragraph after image.</p>
          <p>Final trailing paragraph.</p>
        </article>
      </body>
    </html>
    """

    async def fake_query_entries(_client, _search_query, _max_results):
        return [entry]

    async def fake_request(_client, _method, _url, **_kwargs):
        return _FakeResponse(html)

    monkeypatch.setattr(tool, "_query_entries", fake_query_entries)
    monkeypatch.setattr(tool, "_make_request_with_retry", fake_request)

    result = await tool.load_paper_html_text(title="Test Paper")
    extracted_text = _extract_summary_payload(result)

    assert "Paper Heading" in extracted_text
    assert "First paragraph before image." in extracted_text
    assert "Second paragraph after image." in extracted_text
    assert "Final trailing paragraph." in extracted_text


@pytest.mark.asyncio
async def test_load_paper_html_text_respects_explicit_max_chars(monkeypatch):
    tool = Tools()
    entry = {
        "title": "Test Paper",
        "authors": "A. Author",
        "published": "2026-01-01",
        "category": "astro-ph",
        "id": "2401.00001v1",
    }

    html = """
    <html>
      <body>
        <article>
          <h1>Paper Heading</h1>
          <p>First paragraph before image.</p>
          <img src=\"figure1.png\" />
          <p>Second paragraph after image.</p>
          <p>Final trailing paragraph.</p>
        </article>
      </body>
    </html>
    """

    async def fake_query_entries(_client, _search_query, _max_results):
        return [entry]

    async def fake_request(_client, _method, _url, **_kwargs):
        return _FakeResponse(html)

    monkeypatch.setattr(tool, "_query_entries", fake_query_entries)
    monkeypatch.setattr(tool, "_make_request_with_retry", fake_request)

    result = await tool.load_paper_html_text(title="Test Paper", max_chars=45)
    extracted_text = _extract_summary_payload(result)

    assert len(extracted_text) <= 45
    assert "Paper Heading" in extracted_text
    assert "Final trailing paragraph." not in extracted_text
