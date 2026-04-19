"""
Web crawler and ingestion script for ChromaDB RAG.

Crawls a website using Scrapy, extracts text from HTML pages, PDFs, and DOCX
files, then ingests the results directly into ChromaDB.

Usage:
    python scripts/web_ingest.py --url https://www.sunrisesd.ca
    python scripts/web_ingest.py --url https://www.sunrisesd.ca --depth 3
    python scripts/web_ingest.py --url https://www.sunrisesd.ca --delay 1.0
    python scripts/web_ingest.py --url https://www.sunrisesd.ca --clear

Options:
    --url               Start URL to crawl (required)
    --depth             Maximum crawl depth (default: 3)
    --delay             Seconds between requests, be polite! (default: 0.5)
    --chunk-size        Characters per chunk (default: 500)
    --chunk-overlap     Overlap between chunks (default: 50)
    --clear             Wipe the collection before ingesting
    --dry-run           Print what would be ingested without writing to ChromaDB
    --login-url         URL of the login form to POST to before crawling
    --username          Username / email for login (use with --login-url)
    --password          Password for login (use with --login-url)
    --login-user-field  Form field name for username (default: log  — WordPress default)
    --login-pass-field  Form field name for password (default: pwd  — WordPress default)

Login example (WordPress / SiteGround):
    python scripts/web_ingest.py --url https://members.example.com \\
        --login-url https://members.example.com/wp-login.php \\
        --username myuser --password mypassword
"""
from __future__ import annotations

import argparse
import hashlib
import io
import logging
import sys
import tempfile
import threading
from pathlib import Path
from urllib.parse import urljoin, urlparse

# Allow running from repo root
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import settings
from app.services.retriever import Retriever

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).parent.parent


# ---------------------------------------------------------------------------
# Text extraction helpers
# ---------------------------------------------------------------------------

def _extract_html(html: bytes | str) -> str:
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml")
    # Remove boilerplate tags
    for tag in soup(["script", "style", "nav", "footer", "header", "noscript"]):
        tag.decompose()
    return soup.get_text(separator="\n", strip=True)


def _extract_pdf(data: bytes) -> str:
    import pdfplumber
    text_parts: list[str] = []
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                text_parts.append(text)
    return "\n\n".join(text_parts)


def _extract_docx(data: bytes) -> str:
    import docx
    doc = docx.Document(io.BytesIO(data))
    return "\n".join(p.text for p in doc.paragraphs if p.text.strip())


# ---------------------------------------------------------------------------
# Chunking
# ---------------------------------------------------------------------------

def _chunk_text(text: str, chunk_size: int, overlap: int) -> list[str]:
    chunks: list[str] = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunks.append(text[start:end].strip())
        start += chunk_size - overlap
    return [c for c in chunks if c]


def _stable_id(url: str, chunk_index: int) -> str:
    raw = f"{url}::{chunk_index}"
    return hashlib.md5(raw.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Scrapy spider (runs in a thread)
# ---------------------------------------------------------------------------

class _TextItem:
    """Simple container passed from spider to main thread."""
    __slots__ = ("url", "text", "content_type")

    def __init__(self, url: str, text: str, content_type: str) -> None:
        self.url = url
        self.text = text
        self.content_type = content_type


def _run_spider(
    start_url: str,
    allowed_domain: str,
    max_depth: int,
    delay: float,
    items: list[_TextItem],
    errors: list[str],
    login_url: str | None = None,
    login_user_field: str = "log",
    login_pass_field: str = "pwd",
    username: str | None = None,
    password: str | None = None,
) -> None:
    """Run the Scrapy spider synchronously in a worker thread."""
    import scrapy
    from scrapy.crawler import CrawlerProcess
    from scrapy.http import Response

    class SiteSpider(scrapy.Spider):
        name = "site_spider"
        allowed_domains = [allowed_domain]
        start_urls = [start_url]
        custom_settings = {
            "DOWNLOAD_DELAY": delay,
            "DEPTH_LIMIT": max_depth,
            "ROBOTSTXT_OBEY": True,
            "COOKIES_ENABLED": True,
            "USER_AGENT": "SoleilAI-RAG-Crawler/1.0 (research; contact admin@sunrisesd.ca)",
            "LOG_LEVEL": "WARNING",
            "HTTPCACHE_ENABLED": False,
            # Accept HTML and common document types
            "ACCEPT_TYPES": ["text/html", "application/pdf",
                             "application/vnd.openxmlformats-officedocument"
                             ".wordprocessingml.document"],
        }

        async def start(self):
            if login_url and username and password:
                yield scrapy.Request(login_url, callback=self._do_login, dont_filter=True)
            else:
                for url in self.start_urls:
                    yield scrapy.Request(url, callback=self.parse)

        def _do_login(self, response: Response):
            # Capture hidden fields (nonce, redirect_to, etc.) from the form
            hidden = dict(zip(
                response.css("input[type=hidden]::attr(name)").getall(),
                response.css("input[type=hidden]::attr(value)").getall(),
            ))
            form_data = {**hidden, login_user_field: username, login_pass_field: password}
            # Resolve the form action against the current page URL;
            # fall back to the login_url if the action attribute is absent or empty
            raw_action = response.css("form::attr(action)").get("") or ""
            action = response.urljoin(raw_action) if raw_action else login_url
            logger.info("Submitting login form to %s", action)
            yield scrapy.FormRequest(url=action, formdata=form_data,
                                     callback=self._after_login)

        def _after_login(self, response: Response):
            if "incorrect" in response.text.lower() or "error" in response.url.lower():
                logger.error("Login appears to have failed at %s — check credentials",
                             response.url)
            else:
                logger.info("Login succeeded, starting crawl from %s", start_url)
            for url in self.start_urls:
                yield scrapy.Request(url, callback=self.parse, dont_filter=True)

        def parse(self, response: Response):
            content_type = response.headers.get("Content-Type", b"").decode().split(";")[0].strip()

            try:
                if "pdf" in content_type:
                    text = _extract_pdf(response.body)
                    if text.strip():
                        items.append(_TextItem(response.url, text, "pdf"))

                elif "wordprocessingml" in content_type or response.url.endswith(".docx"):
                    text = _extract_docx(response.body)
                    if text.strip():
                        items.append(_TextItem(response.url, text, "docx"))

                else:
                    # Default: treat as HTML
                    text = _extract_html(response.body)
                    if text.strip():
                        items.append(_TextItem(response.url, text, "html"))

                    # Follow links on HTML pages
                    for href in response.css("a::attr(href)").getall():
                        absolute = urljoin(response.url, href)
                        parsed = urlparse(absolute)
                        if parsed.netloc == allowed_domain or parsed.netloc == "":
                            yield response.follow(href, callback=self.parse)

            except Exception as exc:
                err = f"{response.url}: {exc}"
                errors.append(err)
                logger.warning("Extraction error — %s", err)

    process = CrawlerProcess()
    process.crawl(SiteSpider)
    process.start()  # blocks until complete


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Crawl a website and ingest into ChromaDB")
    parser.add_argument("--url", required=True, help="Start URL to crawl")
    parser.add_argument("--depth", type=int, default=3, help="Maximum crawl depth (default: 3)")
    parser.add_argument("--delay", type=float, default=0.5,
                        help="Seconds between requests (default: 0.5)")
    parser.add_argument("--chunk-size", type=int, default=500,
                        help="Characters per chunk (default: 500)")
    parser.add_argument("--chunk-overlap", type=int, default=50,
                        help="Overlap between chunks (default: 50)")
    parser.add_argument("--clear", action="store_true",
                        help="Wipe the collection before ingesting")
    parser.add_argument("--login-url", default=None,
                        help="URL of the login form (e.g. https://example.com/wp-login.php)")
    parser.add_argument("--username", default=None,
                        help="Username or email for login")
    parser.add_argument("--password", default=None,
                        help="Password for login")
    parser.add_argument("--login-user-field", default="log",
                        help="Form field name for username (default: log — WordPress)")
    parser.add_argument("--login-pass-field", default="pwd",
                        help="Form field name for password (default: pwd — WordPress)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print page count without writing to ChromaDB")
    args = parser.parse_args()

    # Normalise backslashes (common when copy-pasting URLs in PowerShell)
    url = args.url.replace("\\", "/")
    parsed = urlparse(url)
    allowed_domain = parsed.netloc
    if not allowed_domain or parsed.scheme not in ("http", "https"):
        logger.error("Invalid URL: %s  (tip: use forward slashes, e.g. https://example.com)", url)
        sys.exit(1)
    args.url = url  # propagate normalised URL

    # ------------------------------------------------------------------
    # Initialise retriever (unless dry-run)
    # ------------------------------------------------------------------
    retriever: Retriever | None = None
    if not args.dry_run:
        retriever = Retriever(
            db_path=str(_REPO_ROOT / settings.chroma_db_path),
            collection_name=settings.chroma_collection,
            embedding_model=settings.embedding_model,
            top_k=settings.rag_top_k,
        )
        retriever.start()
        if not retriever.available:
            logger.error("ChromaDB could not be initialised — is chromadb installed?")
            sys.exit(1)

        if args.clear:
            import chromadb
            client = chromadb.PersistentClient(
                path=str(_REPO_ROOT / settings.chroma_db_path)
            )
            client.delete_collection(settings.chroma_collection)
            retriever.start()
            logger.info("Collection cleared")

    if args.login_url and not (args.username and args.password):
        logger.error("--login-url requires both --username and --password")
        sys.exit(1)
    if args.login_url:
        logger.info("Authentication enabled — will log in at %s", args.login_url)

    # ------------------------------------------------------------------
    # Crawl
    # ------------------------------------------------------------------
    logger.info("Starting crawl of %s (depth=%d, delay=%.1fs)", args.url, args.depth, args.delay)

    items: list[_TextItem] = []
    errors: list[str] = []

    _run_spider(
        args.url, allowed_domain, args.depth, args.delay, items, errors,
        login_url=args.login_url,
        login_user_field=args.login_user_field,
        login_pass_field=args.login_pass_field,
        username=args.username,
        password=args.password,
    )

    logger.info("Crawl complete — %d page(s) fetched, %d error(s)", len(items), len(errors))

    if args.dry_run:
        for item in items:
            chunks = _chunk_text(item.text, args.chunk_size, args.chunk_overlap)
            print(f"  [{item.content_type}] {item.url}  →  {len(chunks)} chunk(s)")
        print(f"\nTotal pages: {len(items)}")
        return

    # ------------------------------------------------------------------
    # Ingest into ChromaDB
    # ------------------------------------------------------------------
    total_chunks = 0
    for item in items:
        chunks = _chunk_text(item.text, args.chunk_size, args.chunk_overlap)
        if not chunks:
            continue
        ids = [_stable_id(item.url, i) for i in range(len(chunks))]
        metadatas = [
            {"source": item.url, "content_type": item.content_type, "chunk": i}
            for i in range(len(chunks))
        ]
        retriever.add_documents(chunks, ids, metadatas)
        total_chunks += len(chunks)
        logger.info("[%s] %s → %d chunk(s)", item.content_type, item.url, len(chunks))

    logger.info(
        "Ingestion complete — %d chunk(s) added (collection total: %d)",
        total_chunks,
        retriever.document_count,
    )
    if errors:
        logger.warning("%d page(s) had extraction errors:", len(errors))
        for e in errors:
            logger.warning("  %s", e)


if __name__ == "__main__":
    main()
