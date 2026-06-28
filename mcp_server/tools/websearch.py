from __future__ import annotations

import json
import logging
import os
import re
from html import unescape
from typing import Any, Literal
from urllib.parse import quote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT_SECONDS = 20
_SUPPORTED_FOCUS = {
    "all",
    "web",
    "news",
    "wikipedia",
    "academia",
    "reddit",
    "images",
    "videos",
}
_TAG_RE = re.compile(r"<.*?>")
_WORD_RE = re.compile(r"[a-z0-9]+")
_SITE_QUERY_HINTS = ("website", "web site", "site", "webpage", "page", "pages")
_QUERY_STOPWORDS = {
    "the",
    "and",
    "for",
    "from",
    "that",
    "this",
    "their",
    "there",
    "what",
    "which",
    "when",
    "where",
    "with",
    "have",
    "about",
    "into",
    "onto",
    "your",
    "they",
    "them",
    "site",
    "website",
    "page",
    "pages",
    "web",
}
_FRESHNESS_HINTS = {"current", "latest", "recent", "upcoming", "today", "now", "advertising", "advertised"}
_MONTH_NAMES = (
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
)


def _normalize_url(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    return parsed._replace(fragment="", query="", path=path).geturl()


def _tokenize(text: str) -> list[str]:
    return _WORD_RE.findall((text or "").lower())


def _query_terms(query: str) -> set[str]:
    return {term for term in _tokenize(query) if len(term) > 2 and term not in _QUERY_STOPWORDS}


def _looks_like_site_content_query(query: str) -> bool:
    lowered = (query or "").lower()
    return any(term in lowered for term in _SITE_QUERY_HINTS)


def _same_domain(left: str, right: str) -> bool:
    return urlparse(left).netloc.lower() == urlparse(right).netloc.lower()


def _is_root_page(url: str) -> bool:
    path = (urlparse(url).path or "/").strip()
    return path in {"", "/"}


def _fetch_html(url: str) -> str | None:
    response = requests.get(
        url,
        headers={"User-Agent": "AstroLlama/1.0 (+websearch)"},
        timeout=_DEFAULT_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return getattr(response, "text", None)


def _score_candidate_target(text: str, query_terms: set[str]) -> int:
    lowered = (text or "").lower()
    score = sum(3 for term in query_terms if term in lowered)
    score += sum(2 for term in _FRESHNESS_HINTS if term in lowered)
    return score


def _extract_candidate_urls(base_url: str, html: str, query: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    query_terms = _query_terms(query)
    scored_urls: list[tuple[int, str]] = []
    seen: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href") or "").strip()
        if not href or href.startswith(("#", "mailto:", "javascript:")):
            continue
        absolute = _normalize_url(urljoin(base_url, href))
        if not absolute.startswith(("http://", "https://")):
            continue
        if not _same_domain(base_url, absolute):
            continue
        anchor_text = _clean_text(anchor.get_text(" ", strip=True))
        nav_bonus = 0
        parent_names = [parent.name for parent in anchor.parents if getattr(parent, "name", None)]
        if any(name in {"nav", "header", "menu", "aside"} for name in parent_names[:4]):
            nav_bonus += 4
        path = urlparse(absolute).path.strip("/")
        if path and "/" not in path:
            nav_bonus += 2
        score = nav_bonus + _score_candidate_target(f"{absolute} {anchor_text}", query_terms)
        if absolute in seen:
            continue
        seen.add(absolute)
        scored_urls.append((score, absolute))

    scored_urls.sort(key=lambda item: (-item[0], len(item[1])))
    return [url for _, url in scored_urls[:6]]


def _extract_page_summary(html: str, query: str) -> tuple[str, list[str], str]:
    soup = BeautifulSoup(html, "html.parser")
    query_terms = _query_terms(query)
    candidates: list[tuple[int, str]] = []
    seen_lines: set[str] = set()
    page_title = _clean_text(soup.title.string if soup.title and soup.title.string else "")

    meta_desc = soup.find("meta", attrs={"name": re.compile("description", re.IGNORECASE)})
    meta_text = _clean_text(meta_desc.get("content")) if meta_desc and meta_desc.get("content") else ""
    if meta_text:
        candidates.append((max(1, _score_candidate_target(meta_text, query_terms)), meta_text))

    for tag in soup.find_all(["h1", "h2", "h3", "li", "p"]):
        line = _clean_text(tag.get_text(" ", strip=True))
        if len(line) < 20 or len(line) > 260:
            continue
        normalized = line.lower()
        if normalized in seen_lines:
            continue
        seen_lines.add(normalized)
        score = _score_candidate_target(line, query_terms)
        score += sum(2 for month in _MONTH_NAMES if month in normalized)
        score += 2 if re.search(r"\b\d{1,2}(?::\d{2})?\s*(am|pm)\b", normalized) else 0
        score += 2 if re.search(r"\b20\d{2}\b", normalized) else 0
        if tag.name in {"h1", "h2", "h3"}:
            score += 1
        if score <= 0 and len(candidates) >= 6:
            continue
        candidates.append((score, line))

    candidates.sort(key=lambda item: (-item[0], len(item[1])))
    top_lines = [line for _, line in candidates[:5] if line]
    if not top_lines:
        fallback_lines = []
        for tag in soup.find_all(["h1", "h2", "p"], limit=4):
            line = _clean_text(tag.get_text(" ", strip=True))
            if len(line) >= 20 and line not in fallback_lines:
                fallback_lines.append(line)
        top_lines = fallback_lines
    return (" ".join(top_lines), top_lines, page_title)


def _select_best_candidate(candidate_pages: list[dict[str, Any]], query: str) -> dict[str, Any]:
    best = candidate_pages[0]
    if not _is_root_page(str(best.get("url") or "")):
        return best

    query_terms = _query_terms(query)
    if not query_terms:
        return best

    for candidate in candidate_pages[1:]:
        candidate_url = str(candidate.get("url") or "")
        if _is_root_page(candidate_url):
            continue
        # Prefer a deeper page when it's close in quality to homepage content.
        if int(candidate.get("score") or 0) >= int(best.get("score") or 0) - 3:
            return candidate
    return best


def _enrich_site_content_results(query: str, results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not _looks_like_site_content_query(query):
        return results

    enriched_results = [dict(item) for item in results]
    for index, item in enumerate(enriched_results):
        if item.get("type") != "web":
            continue
        url = str(item.get("url") or "").strip()
        if not url:
            continue
        try:
            homepage_html = _fetch_html(url)
            if not homepage_html:
                continue
            candidate_urls = [url]
            candidate_urls.extend(_extract_candidate_urls(url, homepage_html, query))
            seen_candidates: set[str] = set()
            candidate_pages: list[dict[str, Any]] = []
            for candidate_url in candidate_urls[:5]:
                normalized_candidate = _normalize_url(candidate_url)
                if normalized_candidate in seen_candidates:
                    continue
                seen_candidates.add(normalized_candidate)
                try:
                    candidate_html = homepage_html if normalized_candidate == _normalize_url(url) else _fetch_html(candidate_url)
                except requests.RequestException:
                    continue
                if not candidate_html:
                    continue
                summary_text, summary_lines, page_title = _extract_page_summary(candidate_html, query)
                if not summary_lines:
                    continue
                score = _score_candidate_target(f"{normalized_candidate} {page_title} {summary_text}", _query_terms(query))
                score += len(summary_lines) * 2
                if normalized_candidate != _normalize_url(url):
                    score += 1
                candidate_pages.append(
                    {
                        "score": score,
                        "url": normalized_candidate,
                        "title": page_title,
                        "summary": summary_text,
                        "summary_lines": summary_lines,
                    }
                )
            candidate_pages.sort(key=lambda candidate: (-candidate["score"], len(candidate["url"])))
            if candidate_pages:
                best_match = _select_best_candidate(candidate_pages, query)
                item["matched_page_url"] = best_match["url"]
                if best_match.get("title"):
                    item["matched_page_title"] = best_match["title"]
                item["site_summary"] = best_match["summary"]
                item["site_summary_lines"] = best_match["summary_lines"]
                item["site_candidates"] = candidate_pages[:3]
                item["crawl_strategy"] = "same-domain candidate page crawl"
                enriched_results[index] = item
                break
        except requests.RequestException as exc:
            logger.debug("Site content enrichment failed for %s: %s", url, exc)

    return enriched_results


def _clean_text(text: str | None) -> str:
    return _TAG_RE.sub("", unescape(str(text or ""))).strip()


def _decode_web_payload(data: dict[str, Any]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    for result in data.get("infobox", {}).get("results", []):
        attributes = result.get("attributes") or []
        results.append(
            {
                "type": "infobox",
                "url": result.get("url") or "",
                "description": _clean_text(result.get("description")),
                "long_desc": _clean_text(result.get("long_desc")),
                "attributes": {
                    str(attr[0]): _clean_text(attr[1])
                    for attr in attributes
                    if isinstance(attr, (list, tuple)) and len(attr) >= 2
                },
            }
        )

    for result in (data.get("web", {}).get("results") or [])[:8]:
        item = {
            "type": "web",
            "title": _clean_text(result.get("title")),
            "age": result.get("age") or "",
            "description": _clean_text(result.get("description")),
            "url": (result.get("profile") or {}).get("url") or result.get("url") or "",
        }
        article = result.get("article") or {}
        if article:
            item.update(
                {
                    "author": article.get("author") or "",
                    "published": article.get("date") or "",
                    "publisher_type": ((article.get("publisher") or {}).get("type") or ""),
                    "publisher_name": ((article.get("publisher") or {}).get("name") or ""),
                }
            )
        snippets = [_clean_text(snippet) for snippet in (result.get("extra_snippets") or [])]
        snippets = [snippet for snippet in snippets if snippet]
        if snippets:
            item["deep_results"] = snippets
        results.append(item)

    for result in data.get("news", {}).get("results") or []:
        item = {
            "type": "news",
            "title": _clean_text(result.get("title")),
            "age": result.get("age") or "",
            "description": _clean_text(result.get("description")),
            "url": (result.get("profile") or {}).get("url") or result.get("url") or "",
        }
        snippets = [_clean_text(snippet) for snippet in (result.get("extra_snippets") or [])]
        snippets = [snippet for snippet in snippets if snippet]
        if snippets:
            item["deep_results"] = snippets
        results.append(item)

    for result in (data.get("videos", {}).get("results") or [])[:4]:
        item = {
            "type": "videos",
            "description": _clean_text(result.get("description")),
            "url": (result.get("profile") or {}).get("url") or result.get("url") or "",
        }
        snippets = [_clean_text(snippet) for snippet in (result.get("extra_snippets") or [])]
        snippets = [snippet for snippet in snippets if snippet]
        if snippets:
            item["deep_results"] = snippets
        results.append(item)

    return results


def _decode_image_payload(data: dict[str, Any]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for result in data.get("results") or []:
        image_props = result.get("properties") or {}
        results.append(
            {
                "type": "image",
                "title": _clean_text(result.get("title")),
                "url": result.get("url") or "",
                "page_fetched": result.get("page_fetched") or "",
                "image_url": image_props.get("url") or "",
            }
        )
    return results


def _build_query(query: str, focus: str) -> tuple[str, str]:
    goggles_id = ""
    effective_query = query.strip()

    if focus == "reddit":
        effective_query = f"site:reddit.com {effective_query}".strip()
    elif focus == "academia":
        goggles_id = (
            "&goggles_id=https://raw.githubusercontent.com/solso/goggles/main/"
            "academic_papers_search.goggle"
        )
    elif focus == "wikipedia":
        effective_query = f"site:wikipedia.org {effective_query}".strip()

    return effective_query, goggles_id


def _search_brave_web(
    query: str,
    country: str,
    language: str,
    focus: str,
    search_key: str,
) -> list[dict[str, Any]]:
    results_filter = ["infobox"]
    if focus in {"all", "web", "reddit", "academia", "wikipedia"}:
        results_filter.append("web")
    if focus in {"all", "news"}:
        results_filter.append("news")
    if focus == "videos":
        results_filter.append("videos")

    effective_query, goggles_id = _build_query(query, focus)
    url = (
        "https://api.search.brave.com/res/v1/web/search"
        f"?q={quote(effective_query)}"
        f"&results_filter={','.join(results_filter)}"
        f"&country={country}"
        f"&search_lang={language}"
        "&text_decorations=no&extra_snippets=true&count=20"
        f"{goggles_id}"
    )
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "X-Subscription-Token": search_key,
    }

    response = requests.get(url, headers=headers, timeout=_DEFAULT_TIMEOUT_SECONDS)
    response.raise_for_status()
    return _decode_web_payload(response.json())


def _search_brave_media(
    query: str,
    country: str,
    language: str,
    focus: Literal["images", "videos"],
    search_key: str,
) -> list[dict[str, Any]]:
    url = (
        f"https://api.search.brave.com/res/v1/{focus}/search"
        f"?q={quote(query.strip())}&country={country}&search_lang={language}&count=10"
    )
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "X-Subscription-Token": search_key,
    }

    response = requests.get(url, headers=headers, timeout=_DEFAULT_TIMEOUT_SECONDS)
    response.raise_for_status()
    data = response.json()
    if focus == "images":
        return _decode_image_payload(data)
    return _decode_web_payload({"videos": data})


def search_web_sync(
    query: str,
    country: str = "US",
    language: str = "en",
    focus: str = "all",
    search_key: str = "",
) -> list[dict[str, Any]] | dict[str, str]:
    normalized_query = str(query or "").strip()
    if not normalized_query:
        return {"error": "Query must not be empty."}

    normalized_focus = (focus or "all").strip().lower()
    if normalized_focus not in _SUPPORTED_FOCUS:
        normalized_focus = "all"

    resolved_key = str(search_key or os.environ.get("BRAVE_SEARCH_TOKEN") or "").strip()
    if not resolved_key:
        return {"error": "Brave Search API key is not configured."}

    try:
        if normalized_focus in {"images", "videos"}:
            return _search_brave_media(
                query=normalized_query,
                country=country,
                language=language,
                focus=normalized_focus,
                search_key=resolved_key,
            )
        return _search_brave_web(
            query=normalized_query,
            country=country,
            language=language,
            focus=normalized_focus,
            search_key=resolved_key,
        )
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else "unknown"
        logger.warning("Brave search HTTP error (%s): %s", status_code, exc)
        return {"error": f"Brave Search request failed with status {status_code}."}
    except requests.RequestException as exc:
        logger.warning("Brave search request error: %s", exc)
        return {"error": "Failed to reach Brave Search."}
    except Exception as exc:
        logger.exception("Unexpected Brave search failure")
        return {"error": f"Unexpected search failure: {exc}"}


class Tools:
    class Valves(BaseModel):
        SEARCH_KEY: str = Field(default="", description="Brave Search API Key")

    def __init__(self) -> None:
        self.valves = self.Valves()
        self.citation = True

    async def search_web(
        self,
        query: str,
        country: str = "US",
        language: str = "en",
        focus: str = "all",
    ) -> str:
        """Search the web for the given query.

        :param query: Search query to send to Brave Search.
        :param country: Two-letter country code for regional search behavior.
        :param language: Language code such as en, fr, or de.
        :param focus: Search focus: all, web, news, wikipedia, academia, reddit, images, or videos.
        :returns: JSON string containing normalized search results or an error payload.
        """
        results = search_web_sync(
            query=query,
            country=country,
            language=language,
            focus=focus,
            search_key=self.valves.SEARCH_KEY,
        )
        if isinstance(results, list):
            results = _enrich_site_content_results(query, results)
        return json.dumps(results)