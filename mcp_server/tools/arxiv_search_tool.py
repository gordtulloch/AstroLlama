"""
title: arXiv Astronomy Search Tool
description: Tool to search official arXiv astronomy papers in the astro-ph categories
author: AstroLlama
version: 0.4.0
"""

import re
from difflib import SequenceMatcher
from typing import Any, Optional, Callable, Awaitable
from urllib.parse import quote
from xml.etree import ElementTree

import httpx
from bs4 import BeautifulSoup
from pydantic import BaseModel


class Tools:
    class UserValves(BaseModel):
        """No API keys required for arXiv search."""

        pass

    def __init__(self):
        self.base_url = "https://export.arxiv.org/api/query"
        self.max_results = 5
        self.citation = False

    @staticmethod
    def _normalize_title(text: str) -> str:
        cleaned = re.sub(r"[^a-z0-9]+", " ", (text or "").lower())
        return " ".join(cleaned.split())

    @staticmethod
    def _strip_version(paper_id: str) -> str:
        return re.sub(r"v\d+$", "", paper_id or "")

    def _paper_urls(self, paper_id: str) -> dict[str, str]:
        base_id = self._strip_version(paper_id)
        return {
            "abstract": f"https://arxiv.org/abs/{paper_id}",
            "pdf": f"https://arxiv.org/pdf/{paper_id}",
            "html": f"https://arxiv.org/html/{base_id}",
            "ar5iv": f"https://ar5iv.labs.arxiv.org/html/{base_id}",
        }

    @staticmethod
    def _escape_markdown(text: str) -> str:
        return text.replace("[", r"\[").replace("]", r"\]")

    def _format_results(self, topic: str, entries: list[dict[str, str]]) -> str:
        lines = [
            f"Latest astronomy papers on '{topic}' in arXiv astro-ph.",
            "Click a title to open its abstract page:",
            "",
        ]

        for index, entry in enumerate(entries, 1):
            title_text = self._escape_markdown(entry.get("title", "Unknown Title"))
            link = entry.get("id")
            urls = self._paper_urls(link) if link else {}
            link_text = urls.get("abstract", "No link available")
            html_text = urls.get("ar5iv", "No link available")
            summarize_link = (
                "astrollama://summarize?"
                f"title={quote(entry.get('title', ''))}"
                f"&paper_id={quote(link or '')}"
            )
            category = entry.get("category", "astro-ph")
            pub_date = entry.get("published", "Unknown Date")
            lines.append(
                f"{index}. [{title_text}]({link_text}) ([HTML text]({html_text})) ([Summarize]({summarize_link}))"
            )
            lines.append(f"   {category} | {pub_date}")

        return "\n".join(lines)

    @staticmethod
    def _build_search_query(topic: str) -> str:
        normalized_topic = " ".join(topic.split())
        return f'all:"{normalized_topic}" AND cat:astro-ph*'

    @staticmethod
    def _build_title_query(title: str, author_hint: str = "") -> str:
        safe_title = " ".join(title.replace('"', " ").split())
        query = f'ti:"{safe_title}"'
        if author_hint.strip():
            surname = author_hint.split()[-1].replace('"', " ").strip()
            if surname:
                query += f' AND au:"{surname}"'
        return query

    @staticmethod
    def _build_general_paper_query(title: str, author_hint: str = "") -> str:
        safe_title = " ".join(title.replace('"', " ").split())
        query = f'all:"{safe_title}"'
        if author_hint.strip():
            surname = author_hint.split()[-1].replace('"', " ").strip()
            if surname:
                query += f' AND au:"{surname}"'
        return query

    @staticmethod
    def _build_keyword_query(title: str, author_hint: str = "") -> str:
        words = [w for w in re.findall(r"[A-Za-z0-9]+", title or "") if len(w) >= 4]
        unique_words: list[str] = []
        seen: set[str] = set()
        for word in words:
            lowered = word.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            unique_words.append(word)
        if not unique_words:
            unique_words = [w for w in re.findall(r"[A-Za-z0-9]+", title or "")][:5]

        joined = " AND ".join(f"all:{w}" for w in unique_words[:8])
        query = joined or "all:arxiv"
        if author_hint.strip():
            surname = author_hint.split()[-1].replace('"', " ").strip()
            if surname:
                query += f' AND au:"{surname}"'
        return query

    @staticmethod
    def _html_unavailable(html_content: str) -> bool:
        body_text = " ".join(html_content.split()).lower()
        return "no html for" in body_text or "html is not available for the source" in body_text

    @staticmethod
    def _truncate_text(lines: list[str], max_chars: int) -> str:
        chunks: list[str] = []
        total = 0
        for line in lines:
            addition = len(line) + (1 if chunks else 0)
            if total + addition > max_chars:
                break
            chunks.append(line)
            total += addition
        return "\n".join(chunks).strip()

    def _extract_html_text(self, html_content: str, max_chars: int) -> str:
        soup = BeautifulSoup(html_content, "html.parser")
        for tag in soup(["script", "style", "noscript", "svg", "math", "table"]):
            tag.decompose()

        container = soup.select_one("article") or soup.select_one("main") or soup.body or soup
        selectors = [".ltx_abstract", "h1", "h2", "h3", "p"]
        lines: list[str] = []
        seen: set[str] = set()

        for node in container.select(", ".join(selectors)):
            text = " ".join(node.get_text(" ", strip=True).split())
            if not text or len(text) < 2:
                continue
            if text.lower().startswith("skip to main content"):
                continue
            if text in seen:
                continue
            seen.add(text)
            if node.name in {"h1", "h2", "h3"}:
                lines.append(text)
            else:
                lines.append(text)

        return self._truncate_text(lines, max_chars)

    def _extract_abs_page_text(self, html_content: str, max_chars: int) -> str:
        soup = BeautifulSoup(html_content, "html.parser")
        lines: list[str] = []

        title = soup.select_one("h1.title")
        if title:
            lines.append(" ".join(title.get_text(" ", strip=True).replace("Title:", "").split()))

        authors = soup.select_one("div.authors")
        if authors:
            lines.append("Authors: " + " ".join(authors.get_text(" ", strip=True).replace("Authors:", "").split()))

        abstract = soup.select_one("blockquote.abstract")
        if abstract:
            lines.append("Abstract")
            lines.append(" ".join(abstract.get_text(" ", strip=True).replace("Abstract:", "").split()))

        for meta_selector, label in (("td.comments", "Comments"), ("td.jref", "Journal reference"), ("td.doi", "DOI")):
            node = soup.select_one(meta_selector)
            if node:
                lines.append(f"{label}: " + " ".join(node.get_text(" ", strip=True).split()))

        return self._truncate_text(lines, max_chars)

    @staticmethod
    def _entry_score(entry: dict[str, str], title: str, author_hint: str) -> float:
        normalized_target = Tools._normalize_title(title)
        normalized_title = Tools._normalize_title(entry.get("title", ""))
        score = SequenceMatcher(None, normalized_target, normalized_title).ratio()

        if normalized_target and normalized_title == normalized_target:
            score += 1.0
        elif normalized_target and normalized_target in normalized_title:
            score += 0.4

        if author_hint.strip():
            surname = author_hint.split()[-1].lower()
            if surname and surname in entry.get("authors", "").lower():
                score += 0.3

        return score

    def _select_best_entry(
        self, entries: list[dict[str, str]], title: str, author_hint: str
    ) -> dict[str, str] | None:
        if not entries:
            return None
        return max(entries, key=lambda entry: self._entry_score(entry, title, author_hint))

    async def _query_entries(
        self, client: httpx.AsyncClient, search_query: str, max_results: int
    ) -> list[dict[str, str]]:
        headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/132.0.0.0 Safari/537.36",
        }
        params = {
            "search_query": search_query,
            "start": 0,
            "max_results": max_results,
            "sortBy": "submittedDate",
            "sortOrder": "descending",
        }
        response = await client.get(self.base_url, params=params, headers=headers)
        response.raise_for_status()
        return self._parse_feed(response.text)

    async def _query_entry_by_id(self, client: httpx.AsyncClient, paper_id: str) -> list[dict[str, str]]:
        headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/132.0.0.0 Safari/537.36",
        }
        normalized = (paper_id or "").strip()
        if not normalized:
            return []
        response = await client.get(
            self.base_url,
            params={"id_list": self._strip_version(normalized)},
            headers=headers,
        )
        response.raise_for_status()
        entries = self._parse_feed(response.text)
        if entries:
            return entries

        response = await client.get(
            self.base_url,
            params={"id_list": normalized},
            headers=headers,
        )
        response.raise_for_status()
        return self._parse_feed(response.text)

    @staticmethod
    def _parse_feed(xml_content: str) -> list[dict[str, str]]:
        namespace = {
            "atom": "http://www.w3.org/2005/Atom",
            "arxiv": "http://arxiv.org/schemas/atom",
        }
        root = ElementTree.fromstring(xml_content)
        entries: list[dict[str, str]] = []

        for entry in root.findall("atom:entry", namespace):
            authors = [
                author.findtext("atom:name", default="", namespaces=namespace).strip()
                for author in entry.findall("atom:author", namespace)
            ]
            paper_id = entry.findtext("atom:id", default="", namespaces=namespace).strip()
            summary = entry.findtext("atom:summary", default="", namespaces=namespace)
            published = entry.findtext(
                "atom:published", default="", namespaces=namespace
            ).strip()

            primary_category = entry.find("arxiv:primary_category", namespace)
            category = ""
            if primary_category is not None:
                category = primary_category.attrib.get("term", "").strip()

            entries.append(
                {
                    "title": entry.findtext(
                        "atom:title", default="Unknown Title", namespaces=namespace
                    ).strip(),
                    "authors": ", ".join(author for author in authors if author)
                    or "Unknown Authors",
                    "abstract": " ".join((summary or "No summary available").split()),
                    "id": paper_id.rsplit("/", 1)[-1] if paper_id else "",
                    "published": published[:10] if published else "Unknown Date",
                    "category": category or "astro-ph",
                }
            )

        return entries

    async def search_papers(
        self,
        topic: str,
        __user__: dict = {},
        __event_emitter__: Optional[Callable[[Any], Awaitable[None]]] = None,
    ) -> str:
        """
        Search the official arXiv API for astronomy papers and return formatted results.

        Args:
            topic: Astronomy topic to search for (e.g., "exoplanet atmospheres")

        Returns:
            Formatted string containing paper details including titles, authors, dates,
            URLs and abstracts.
        """
        if __event_emitter__:
            await __event_emitter__(
                {
                    "type": "status",
                    "data": {
                        "description": "Searching arXiv...",
                        "done": False,
                    },
                }
            )

        try:
            search_query = self._build_search_query(topic)

            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                entries = await self._query_entries(client, search_query, self.max_results)
            if not entries:
                if __event_emitter__:
                    await __event_emitter__(
                        {
                            "type": "status",
                            "data": {"description": "No papers found", "done": True},
                        }
                    )
                return f"No papers found on arXiv related to '{topic}'"

            # Emit citation data while keeping the inline result compact.
            for entry in entries:
                title = entry.get("title")
                title_text = title.strip() if title else "Unknown Title"

                summary = entry.get("abstract")
                summary_text = summary.strip() if summary else "No summary available"

                link = entry.get("id")
                urls = self._paper_urls(link) if link else {}
                citation_source = urls.get("ar5iv") or urls.get("abstract", "")

                # Emit citation data as provided.
                if __event_emitter__:
                    await __event_emitter__(
                        {
                            "type": "citation",
                            "data": {
                                "document": [summary_text],
                                "metadata": [{"source": citation_source}],
                                "source": {"name": title_text},
                            },
                        }
                    )

            results = self._format_results(topic, entries)

            if __event_emitter__:
                await __event_emitter__(
                    {
                        "type": "status",
                        "data": {"description": "Search completed", "done": True},
                    }
                )

            return results

        except httpx.HTTPError as e:
            error_msg = f"Error searching arXiv: {str(e)}"
            if __event_emitter__:
                await __event_emitter__(
                    {"type": "status", "data": {"description": error_msg, "done": True}}
                )
            return error_msg
        except Exception as e:
            error_msg = f"Unexpected error during search: {str(e)}"
            if __event_emitter__:
                await __event_emitter__(
                    {"type": "status", "data": {"description": error_msg, "done": True}}
                )
            return error_msg

    async def load_paper_html_text(
        self,
        title: str,
        paper_id: str = "",
        author_hint: str = "",
        max_chars: int = 8000,
        __user__: dict = {},
        __event_emitter__: Optional[Callable[[Any], Awaitable[None]]] = None,
    ) -> str:
        """
        Find a specific astronomy paper and load bounded HTML text suitable for summarization.

        Use this when the user asks to retrieve, read, or summarize a specific paper by title.

        Args:
            title: Paper title or near-exact title to retrieve.
            paper_id: Optional arXiv id for exact lookup (preferred when available).
            author_hint: Optional author hint such as a surname or initials.
            max_chars: Maximum number of extracted text characters to return inline.

        Returns:
            Paper metadata plus HTML-derived text content for the selected paper.
        """
        if __event_emitter__:
            await __event_emitter__(
                {
                    "type": "status",
                    "data": {"description": "Finding paper on arXiv...", "done": False},
                }
            )

        try:
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                entries: list[dict[str, str]] = []
                if paper_id.strip():
                    entries = await self._query_entry_by_id(client, paper_id)

                if not entries:
                    entries = await self._query_entries(
                        client,
                        self._build_title_query(title, author_hint),
                        max(self.max_results * 2, 8),
                    )
                if not entries:
                    entries = await self._query_entries(
                        client,
                        self._build_general_paper_query(title, author_hint),
                        max(self.max_results * 2, 8),
                    )
                if not entries:
                    entries = await self._query_entries(
                        client,
                        self._build_keyword_query(title, author_hint),
                        max(self.max_results * 3, 12),
                    )

                entry = self._select_best_entry(entries, title, author_hint)
                if not entry:
                    return f"No astronomy paper found on arXiv matching '{title}'."

                urls = self._paper_urls(entry.get("id", ""))
                extracted_text = ""
                source_label = ""
                source_url = ""

                for label, url, extractor in (
                    ("HTML text", urls["ar5iv"], self._extract_html_text),
                    ("Native HTML", urls["html"], self._extract_html_text),
                    ("Abstract page", urls["abstract"], self._extract_abs_page_text),
                ):
                    response = await client.get(url)
                    if response.status_code >= 400:
                        continue
                    if label != "Abstract page" and self._html_unavailable(response.text):
                        continue
                    extracted_text = extractor(response.text, max_chars)
                    if extracted_text:
                        source_label = label
                        source_url = url
                        break

                if not extracted_text:
                    return (
                        f"Found paper '{entry.get('title', 'Unknown Title')}', but could not load HTML text. "
                        f"Abstract page: {urls['abstract']}"
                    )

            if __event_emitter__:
                await __event_emitter__(
                    {
                        "type": "citation",
                        "data": {
                            "document": [extracted_text],
                            "metadata": [{"source": source_url}],
                            "source": {"name": entry.get("title", "Unknown Title")},
                        },
                    }
                )
                await __event_emitter__(
                    {
                        "type": "status",
                        "data": {"description": "Paper text loaded", "done": True},
                    }
                )

            return (
                f"Title: {entry.get('title', 'Unknown Title')}\n"
                f"Authors: {entry.get('authors', 'Unknown Authors')}\n"
                f"Published: {entry.get('published', 'Unknown Date')}\n"
                f"Category: {entry.get('category', 'astro-ph')}\n"
                f"Abstract URL: {urls['abstract']}\n"
                f"HTML URL: {source_url}\n"
                f"Retrieved from: {source_label}\n\n"
                f"HTML text for summarization:\n{extracted_text}"
            )

        except httpx.HTTPError as e:
            error_msg = f"Error loading arXiv paper text: {str(e)}"
            if __event_emitter__:
                await __event_emitter__(
                    {"type": "status", "data": {"description": error_msg, "done": True}}
                )
            return error_msg
        except Exception as e:
            error_msg = f"Unexpected error during search: {str(e)}"
            if __event_emitter__:
                await __event_emitter__(
                    {"type": "status", "data": {"description": error_msg, "done": True}}
                )
            return error_msg
