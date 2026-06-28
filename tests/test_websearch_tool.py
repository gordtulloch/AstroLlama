from __future__ import annotations

import json

import pytest
import requests

from mcp_server.tools.websearch import Tools, _enrich_site_content_results, search_web_sync


class FakeResponse:
    def __init__(self, payload: dict | None = None, status_code: int = 200, text: str = "") -> None:
        self._payload = payload
        self.status_code = status_code
        self.text = text

    def json(self) -> dict:
        return self._payload or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"status {self.status_code}", response=self)


def test_search_web_sync_returns_config_error_without_key(monkeypatch):
    monkeypatch.delenv("BRAVE_SEARCH_TOKEN", raising=False)

    result = search_web_sync("Betelgeuse")

    assert result == {"error": "Brave Search API key is not configured."}


def test_search_web_sync_normalizes_web_results(monkeypatch):
    payload = {
        "infobox": {
            "results": [
                {
                    "url": "https://example.com/info",
                    "description": "<b>Bright</b> star",
                    "long_desc": "<p>Red supergiant</p>",
                    "attributes": [["Distance", "<span>548 ly</span>"]],
                }
            ]
        },
        "web": {
            "results": [
                {
                    "url": "https://example.com/article",
                    "title": "<b>Betelgeuse</b>",
                    "description": "<p>Variable star</p>",
                    "extra_snippets": ["<i>Visible in Orion</i>"],
                }
            ]
        },
        "news": {"results": []},
        "videos": {"results": []},
    }

    monkeypatch.setattr(
        "mcp_server.tools.websearch.requests.get",
        lambda *args, **kwargs: FakeResponse(payload),
    )

    result = search_web_sync("Betelgeuse", search_key="key")

    assert result[0] == {
        "type": "infobox",
        "url": "https://example.com/info",
        "description": "Bright star",
        "long_desc": "Red supergiant",
        "attributes": {"Distance": "548 ly"},
    }
    assert result[1]["type"] == "web"
    assert result[1]["title"] == "Betelgeuse"
    assert result[1]["description"] == "Variable star"
    assert result[1]["deep_results"] == ["Visible in Orion"]


@pytest.mark.asyncio
async def test_search_web_tool_returns_json_string(monkeypatch):
    monkeypatch.setattr(
        "mcp_server.tools.websearch.requests.get",
        lambda *args, **kwargs: FakeResponse({"infobox": {"results": []}, "web": {"results": []}, "news": {"results": []}, "videos": {"results": []}}),
    )
    tool = Tools()
    tool.valves.SEARCH_KEY = "key"

    result = await tool.search_web(query="Betelgeuse")

    assert json.loads(result) == []


def test_enrich_site_content_results_returns_candidate_pages_from_same_domain(monkeypatch):
    homepage_html = """
    <html><head><title>RASC Winnipeg Centre</title></head><body>
    <nav><a href="/events">Events</a><a href="/about">About</a></nav>
    <p>Welcome to the RASC Winnipeg Centre website.</p>
    </body></html>
    """
    events_html = """
    <html><head><title>Upcoming Events</title></head><body>
    <h1>Upcoming Events</h1>
    <p>Join us for observing nights, public star parties, and monthly meetings.</p>
    <li>July 18, 2026 Public Star Party at Oak Hammock Marsh</li>
    <li>August 3, 2026 Centre meeting on summer observing targets</li>
    </body></html>
    """

    def fake_get(url, *args, **kwargs):
        if "api.search.brave.com" in url:
            return FakeResponse(
                {
                    "infobox": {"results": []},
                    "web": {
                        "results": [
                            {
                                "url": "https://winnipeg.rasc.ca/",
                                "title": "RASC Winnipeg Centre",
                                "description": "Astronomy club homepage.",
                            }
                        ]
                    },
                    "news": {"results": []},
                    "videos": {"results": []},
                }
            )
        if url.rstrip("/") == "https://winnipeg.rasc.ca":
            return FakeResponse(text=homepage_html)
        if url.rstrip("/") == "https://winnipeg.rasc.ca/events":
            return FakeResponse(text=events_html)
        return FakeResponse(status_code=404, text="not found")

    monkeypatch.setattr("mcp_server.tools.websearch.requests.get", fake_get)

    base_results = search_web_sync(
        "What current activities is RASC Winnipeg Centre advertising on their web site?",
        search_key="key",
    )
    enriched = _enrich_site_content_results(
        "What current activities is RASC Winnipeg Centre advertising on their web site?",
        base_results,
    )

    assert enriched[0]["matched_page_url"] == "https://winnipeg.rasc.ca/events"
    assert "Public Star Party" in enriched[0]["site_summary"]
    assert "monthly meetings" in enriched[0]["site_summary"]
    assert any(
        candidate["url"] == "https://winnipeg.rasc.ca/events"
        for candidate in enriched[0]["site_candidates"]
    )