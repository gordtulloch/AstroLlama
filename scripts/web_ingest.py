"""
Web crawler and ingestion script for ChromaDB RAG.

Crawls a website using Crawl4AI (headless Chromium via Playwright), extracts
clean Markdown from HTML pages, and ingests the results into ChromaDB.
JavaScript-rendered and dynamically loaded pages are supported natively.

Usage:
    python scripts/web_ingest.py --url https://www.sunrisesd.ca
    python scripts/web_ingest.py --url https://www.sunrisesd.ca --depth 3
    python scripts/web_ingest.py --url https://www.sunrisesd.ca --delay 1.0
    python scripts/web_ingest.py --url https://www.sunrisesd.ca --max-pages 50
    python scripts/web_ingest.py --url https://www.sunrisesd.ca --clear

Options:
    --url               Start URL to crawl (required)
    --depth             Maximum crawl depth (default: 3)
    --delay             Seconds to wait after each page load (default: 0.5)
    --max-pages         Maximum total pages to crawl (default: 0 = unlimited)
    --chunk-size        Characters per chunk (default: 500)
    --chunk-overlap     Overlap between chunks (default: 50)
    --clear             Wipe the collection before ingesting
    --dry-run           Print page count without writing to ChromaDB
    --retries           Retry rounds when anti-bot blocking is detected (default: 0)
    --proxy             Proxy server URL (e.g. http://user:pass@host:8080).
                        When set, each retry round first tries direct then escalates
                        to this proxy. Can be repeated for multiple proxies.
    --stealth           Enable Playwright stealth mode + magic popup handling
    --login-url         URL of the login page to authenticate before crawling
    --username          Username / email for login (use with --login-url)
    --password          Password for login (use with --login-url)
    --login-user-field  name attribute of the username input (default: log — WordPress default)
    --login-pass-field  name attribute of the password input (default: pwd — WordPress default)

Login example (WordPress / SiteGround):
    python scripts/web_ingest.py --url https://members.example.com \\
        --login-url https://members.example.com/wp-login.php \\
        --username myuser --password mypassword
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import logging
import sys
from pathlib import Path
from urllib.parse import urlparse

# Allow running from repo root: python scripts/web_ingest.py
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
# Chunking helpers
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
# Crawl4AI crawl (async)
# ---------------------------------------------------------------------------

async def _crawl(args: argparse.Namespace) -> list[tuple[str, str]]:
    """Crawl *args.url* and return a list of (page_url, markdown_text) pairs."""
    from crawl4ai import AsyncWebCrawler, CacheMode
    from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig, ProxyConfig
    from crawl4ai.deep_crawling import BFSDeepCrawlStrategy
    from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy
    from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
    from crawl4ai.content_filter_strategy import PruningContentFilter

    md_generator = DefaultMarkdownGenerator(
        content_filter=PruningContentFilter(threshold=0.4, threshold_type="fixed")
    )

    deep_strategy = BFSDeepCrawlStrategy(
        max_depth=args.depth,
        include_external=False,
        max_pages=args.max_pages if args.max_pages else None,
    )

    # ---- Proxy escalation list --------------------------------------------
    # If one or more --proxy values are given, build an ordered list that tries
    # direct first, then each proxy in turn.  Each retry round repeats the
    # whole list, so worst-case attempts = (1 + retries) × len(proxy_config).
    proxy_config: list[ProxyConfig] | None = None
    if args.proxy:
        proxy_config = [ProxyConfig.DIRECT] + [
            ProxyConfig(server=p) for p in args.proxy
        ]
        logger.info(
            "Proxy escalation: direct → %s  (retries=%d)",
            ", ".join(args.proxy),
            args.retries,
        )

    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        deep_crawl_strategy=deep_strategy,
        scraping_strategy=LXMLWebScrapingStrategy(),
        markdown_generator=md_generator,
        delay_before_return_html=args.delay,
        stream=True,
        verbose=False,
        # Anti-bot retry / proxy escalation
        max_retries=args.retries,
        proxy_config=proxy_config,
        # Stealth helpers recommended for anti-bot sites
        magic=args.stealth,
        wait_until="load" if args.stealth else "domcontentloaded",
    )

    browser_config = BrowserConfig(
        headless=True,
        enable_stealth=args.stealth,
        user_agent=(
            "AstroLlama-RAG-Crawler/2.0 "
            "(headless; research use; contact admin if you have concerns)"
        ),
    )

    pages: list[tuple[str, str]] = []

    # ---- Optional login hook via Playwright --------------------------------
    if args.login_url and args.username and args.password:
        user_selector = f"input[name='{args.login_user_field}']"
        pass_selector = f"input[name='{args.login_pass_field}']"

        async def _login_hook(page, context, **kwargs):
            logger.info("Authenticating at %s", args.login_url)
            try:
                await page.goto(args.login_url, wait_until="domcontentloaded")
                await page.fill(user_selector, args.username)
                await page.fill(pass_selector, args.password)
                await page.click("input[type=submit], button[type=submit]")
                await page.wait_for_load_state("domcontentloaded")
                content = await page.content()
                if "incorrect" in content.lower() or "invalid" in content.lower():
                    logger.warning("Login may have failed — check credentials")
                else:
                    logger.info("Login succeeded, proceeding with crawl")
            except Exception as exc:
                logger.error("Login hook error: %s", exc)
            return page

    # ---- Crawl -------------------------------------------------------------
    async with AsyncWebCrawler(config=browser_config) as crawler:
        if args.login_url and args.username and args.password:
            crawler.crawler_strategy.set_hook("on_page_context_created", _login_hook)

        async for result in await crawler.arun(args.url, config=run_config):
            if not result.success:
                stats = getattr(result, "crawl_stats", {})
                logger.warning(
                    "Failed: %s — %s (attempts=%s, resolved_by=%s)",
                    result.url,
                    result.error_message,
                    stats.get("attempts", "?"),
                    stats.get("resolved_by", "none"),
                )
                continue
            text = (result.markdown.fit_markdown if result.markdown else "") or ""
            if text.strip():
                pages.append((result.url, text))
                logger.info("[html] %s (%d chars)", result.url, len(text))

    return pages


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Crawl a website and ingest into ChromaDB")
    parser.add_argument("--url", required=True, help="Start URL to crawl")
    parser.add_argument("--depth", type=int, default=3,
                        help="Maximum crawl depth (default: 3)")
    parser.add_argument("--delay", type=float, default=0.5,
                        help="Seconds to wait after each page load (default: 0.5)")
    parser.add_argument("--max-pages", type=int, default=0,
                        help="Maximum pages to crawl (default: 0 = unlimited)")
    parser.add_argument("--chunk-size", type=int, default=500,
                        help="Characters per chunk (default: 500)")
    parser.add_argument("--chunk-overlap", type=int, default=50,
                        help="Overlap between chunks (default: 50)")
    parser.add_argument("--clear", action="store_true",
                        help="Wipe the collection before ingesting")
    parser.add_argument("--login-url", default=None,
                        help="URL of the login page (e.g. https://example.com/wp-login.php)")
    parser.add_argument("--username", default=None,
                        help="Username or email for login")
    parser.add_argument("--password", default=None,
                        help="Password for login")
    parser.add_argument("--login-user-field", default="log",
                        help="name attribute of the username input (default: log — WordPress)")
    parser.add_argument("--login-pass-field", default="pwd",
                        help="name attribute of the password input (default: pwd — WordPress)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print page count without writing to ChromaDB")
    parser.add_argument("--retries", type=int, default=0,
                        help="Retry rounds when anti-bot blocking is detected (default: 0)")
    parser.add_argument("--proxy", action="append", default=[],
                        metavar="URL",
                        help="Proxy server URL (repeatable). Each retry round tries direct "
                             "first then escalates through the list in order.")
    parser.add_argument("--stealth", action="store_true",
                        help="Enable Playwright stealth mode and magic popup handling")
    args = parser.parse_args()

    # Normalise backslashes (common when copy-pasting URLs in PowerShell)
    url = args.url.replace("\\", "/")
    parsed = urlparse(url)
    if not parsed.netloc or parsed.scheme not in ("http", "https"):
        logger.error("Invalid URL: %s  (tip: use https://example.com)", url)
        sys.exit(1)
    args.url = url

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
            hf_token=settings.hf_token,
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
    logger.info(
        "Starting crawl of %s (depth=%d, delay=%.1fs%s)",
        args.url, args.depth, args.delay,
        f", max-pages={args.max_pages}" if args.max_pages else "",
    )

    pages = asyncio.run(_crawl(args))

    logger.info("Crawl complete — %d page(s) fetched", len(pages))

    if args.dry_run:
        for page_url, text in pages:
            chunks = _chunk_text(text, args.chunk_size, args.chunk_overlap)
            print(f"  [html] {page_url}  →  {len(chunks)} chunk(s)")
        print(f"\nTotal pages: {len(pages)}")
        return

    # ------------------------------------------------------------------
    # Ingest into ChromaDB
    # ------------------------------------------------------------------
    total_chunks = 0
    for page_url, text in pages:
        chunks = _chunk_text(text, args.chunk_size, args.chunk_overlap)
        if not chunks:
            continue
        ids = [_stable_id(page_url, i) for i in range(len(chunks))]
        metadatas = [
            {"source": page_url, "content_type": "html", "chunk": i}
            for i in range(len(chunks))
        ]
        retriever.add_documents(chunks, ids, metadatas)
        total_chunks += len(chunks)
        logger.info("[html] %s → %d chunk(s)", page_url, len(chunks))

    logger.info(
        "Ingestion complete — %d chunk(s) added (collection total: %d)",
        total_chunks,
        retriever.document_count,
    )


if __name__ == "__main__":
    main()
