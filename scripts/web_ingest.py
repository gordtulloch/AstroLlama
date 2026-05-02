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
    --test-run          Crawl and extract as normal but write results to
                        data/test_run_web_<timestamp>.txt instead of ChromaDB
    --retries           Retry rounds when anti-bot blocking is detected (default: 0)
    --proxy             Proxy server URL (e.g. http://user:pass@host:8080).
                        When set, each retry round first tries direct then escalates
                        to this proxy. Can be repeated for multiple proxies.
    --stealth           Enable Playwright stealth mode + magic popup handling
    --skip-url          URL substring to exclude (repeatable, e.g. /tag/ /author/)
    --pdf               Also download and ingest PDF files linked from crawled pages
    --pdf-columns       Number of columns for PDF OCR layout (default: 1)
    --docx              Also download and ingest Word (.docx) files linked from crawled pages
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

# Silence verbose HTTP/browser library loggers
for _noisy in ("httpx", "httpcore", "hpack", "playwright", "asyncio"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

_REPO_ROOT = Path(__file__).parent.parent


# ---------------------------------------------------------------------------
# PDF extraction helpers (mirrors ingest.py)
# ---------------------------------------------------------------------------

def _ocr_pdf_page(pdf_bytes: bytes, page_index: int, columns: int = 1) -> str:
    """Render one PDF page to an image and return OCR'd text."""
    import io
    import fitz
    import pytesseract
    from PIL import Image

    if sys.platform == "win32":
        _win_default = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
        if Path(_win_default).exists():
            pytesseract.pytesseract.tesseract_cmd = _win_default

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    page = doc[page_index]
    pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
    img = Image.open(io.BytesIO(pix.tobytes("png")))

    if columns <= 1:
        return pytesseract.image_to_string(img)

    width, height = img.size
    strip_w = width // columns
    texts: list[str] = []
    for col in range(columns):
        left = col * strip_w
        right = width if col == columns - 1 else left + strip_w
        texts.append(pytesseract.image_to_string(img.crop((left, 0, right, height))))
    return "\n\n".join(t for t in texts if t.strip())


def _read_pdf_bytes(pdf_bytes: bytes, columns: int = 1) -> str:
    """Extract text from PDF bytes using pdfplumber; OCR image-bearing pages."""
    import io
    import pdfplumber

    pages: list[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            if page.images:
                try:
                    ocr_text = _ocr_pdf_page(pdf_bytes, page.page_number - 1, columns=columns).strip()
                    if ocr_text and ocr_text not in text:
                        text = (text + "\n" + ocr_text).strip()
                except Exception as exc:
                    logger.warning("OCR failed on PDF page %d: %s", page.page_number, exc)
            if text:
                pages.append(text)
    return "\n".join(pages)


async def _fetch_pdf(url: str, session_cookies: dict | None = None) -> bytes | None:
    """Download a PDF URL and return raw bytes, reusing browser cookies if provided."""
    import httpx
    from urllib.parse import urlsplit, urlunsplit, quote, unquote

    # Normalise: decode any existing percent-encoding then re-encode cleanly
    # to avoid double-encoding (%20 → %2520).
    parts = urlsplit(url)
    encoded_url = urlunsplit(parts._replace(path=quote(unquote(parts.path), safe="/:@!$&'()*+,;=")))

    headers = {"User-Agent": "AstroLlama-RAG-Crawler/2.0"}
    # Pass cookies as a header string so httpx doesn't filter by domain
    if session_cookies:
        headers["Cookie"] = "; ".join(f"{k}={v}" for k, v in session_cookies.items())
    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
            resp = await client.get(encoded_url, headers=headers)
            resp.raise_for_status()
            ct = resp.headers.get("content-type", "")
            logger.info(
                "PDF response: status=%d content-type=%r first-bytes=%r url=%s",
                resp.status_code, ct, resp.content[:16], encoded_url,
            )
            # Detect server returning HTML instead of PDF bytes
            if "html" in ct or resp.content[:5] == b"<html":
                logger.warning(
                    "PDF URL returned HTML (status=%d, content-type=%r) for %s",
                    resp.status_code, ct, url,
                )
                return None
            if "pdf" not in ct and not url.lower().endswith(".pdf"):
                logger.debug("Skipping non-PDF response from %s (%s)", url, ct)
                return None
            return resp.content
    except Exception as exc:
        logger.warning("Could not download PDF %s: %s", url, exc)
        return None


async def _fetch_file_playwright(url: str, ctx) -> bytes | None:
    """Download a file URL via Playwright's APIRequestContext.

    This reuses the live browser session (cookies, TLS fingerprint, headers)
    and bypasses bot-protection challenges that block plain httpx requests.
    Falls back to httpx if the Playwright request fails.
    """
    from urllib.parse import urlsplit, urlunsplit, quote, unquote

    # Normalise URL: decode then re-encode to avoid double-encoding
    parts = urlsplit(url)
    encoded_url = urlunsplit(parts._replace(path=quote(unquote(parts.path), safe="/:@!$&'()*+,;=")))

    # `ctx` may be a Browser rather than a BrowserContext depending on
    # which Crawl4AI attribute was resolved.  BrowserContext has .request;
    # Browser does not but exposes .contexts.
    browser_ctx = ctx
    if not hasattr(ctx, "request"):
        contexts = getattr(ctx, "contexts", [])
        if contexts:
            browser_ctx = contexts[0]
            logger.debug("Resolved BrowserContext from Browser.contexts[0]")
        else:
            logger.warning("Cannot resolve a BrowserContext from %s — falling back to httpx", type(ctx).__name__)
            return await _fetch_pdf(url, session_cookies=None)
    try:
        response = await browser_ctx.request.get(encoded_url)
        body = await response.body()
        ct = response.headers.get("content-type", "")
        logger.info(
            "File response (Playwright): status=%d content-type=%r first-bytes=%r",
            response.status, ct, body[:16],
        )
        if "html" in ct or body[:5] == b"<html":
            logger.warning("File URL returned HTML via Playwright for %s", url)
            # Fall through to httpx fallback
        else:
            return body
    except Exception as exc:
        logger.warning("Playwright fetch failed for %s: %s — trying httpx", url, exc)

    # httpx fallback (last resort)
    return await _fetch_pdf(url, session_cookies=None)


# ---------------------------------------------------------------------------
# Docx extraction helpers (mirrors ingest.py)
# ---------------------------------------------------------------------------

def _read_docx_bytes(docx_bytes: bytes) -> str:
    """Extract text from a .docx file given its raw bytes."""
    import io
    from docx import Document

    doc = Document(io.BytesIO(docx_bytes))
    return "\n".join(p.text for p in doc.paragraphs if p.text.strip())


async def _fetch_docx(url: str, session_cookies: dict | None = None) -> bytes | None:
    """Download a .docx URL and return raw bytes, reusing browser cookies if provided."""
    import httpx
    from urllib.parse import urlsplit, urlunsplit, quote

    parts = urlsplit(url)
    encoded_url = urlunsplit(parts._replace(path=quote(parts.path, safe="/:@!$&'()*+,;=")))

    headers = {"User-Agent": "AstroLlama-RAG-Crawler/2.0"}
    if session_cookies:
        headers["Cookie"] = "; ".join(f"{k}={v}" for k, v in session_cookies.items())
    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
            resp = await client.get(encoded_url, headers=headers)
            resp.raise_for_status()
            ct = resp.headers.get("content-type", "")
            # Detect login-redirect: server returned HTML instead of docx bytes
            if "html" in ct or resp.content[:5] == b"<html":
                logger.warning(
                    "Docx URL returned HTML (likely auth redirect) for %s — cookies may be missing",
                    url,
                )
                return None
            if "wordprocessingml" not in ct and not url.lower().endswith(".docx"):
                logger.debug("Skipping non-docx response from %s (%s)", url, ct)
                return None
            return resp.content
    except Exception as exc:
        logger.warning("Could not download docx %s: %s", url, exc)
        return None


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
    from crawl4ai.deep_crawling.filters import FilterChain, URLPatternFilter
    from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy
    from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
    from crawl4ai.content_filter_strategy import PruningContentFilter

    md_generator = DefaultMarkdownGenerator(
        content_filter=PruningContentFilter(threshold=0.4, threshold_type="fixed")
    )

    # ---- URL filters: always exclude binary/media files from Chromium crawl
    # PDFs are collected separately and downloaded via httpx (_fetch_pdf).
    # Images, audio, video, fonts, archives etc. are never worth visiting as HTML;
    # letting the BFS hit them just produces false "anti-bot" warnings.
    # Also exclude any --skip-url substrings.
    _BINARY_EXTS = [
        "*.pdf", "*.PDF",
        "*.jpg", "*.jpeg", "*.png", "*.gif", "*.webp", "*.svg", "*.ico", "*.bmp", "*.tiff",
        "*.mp3", "*.mp4", "*.wav", "*.ogg", "*.webm", "*.avi", "*.mov",
        "*.zip", "*.gz", "*.tar", "*.rar", "*.7z",
        "*.doc", "*.docx", "*.xls", "*.xlsx", "*.ppt", "*.pptx",
        "*.ttf", "*.woff", "*.woff2", "*.eot",
        "*.exe", "*.dmg", "*.pkg",
    ]
    _exclude_patterns = _BINARY_EXTS + [f"*{s}*" for s in args.skip_url]
    filter_chain = FilterChain([URLPatternFilter(patterns=_exclude_patterns, reverse=True)])

    deep_strategy = BFSDeepCrawlStrategy(
        max_depth=args.depth,
        include_external=False,
        filter_chain=filter_chain,
        **({"max_pages": args.max_pages} if args.max_pages else {}),
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
    pdf_urls: set[str] = set()
    docx_urls: set[str] = set()
    _browser_cookies: dict[str, str] = {}  # captured after crawl for authenticated downloads

    # ---- Optional login hook via Playwright --------------------------------
    if args.login_url and args.username and args.password:
        user_selector = f"input[name='{args.login_user_field}']"
        pass_selector = f"input[name='{args.login_pass_field}']"

        _logged_in = [False]  # mutable container so the inner function can mutate it

        async def _login_hook(page, context, **kwargs):
            if _logged_in[0]:
                return page  # session cookie already set — skip repeated logins
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
                    _logged_in[0] = True
                    # Capture cookies immediately while the context is guaranteed open
                    try:
                        ctx = context or page.context
                        raw_cookies = await ctx.cookies()
                        _browser_cookies.update({c["name"]: c["value"] for c in raw_cookies})
                        logger.info(
                            "Captured %d browser cookie(s) after login: %s",
                            len(_browser_cookies),
                            ", ".join(_browser_cookies.keys()),
                        )
                    except Exception as cookie_exc:
                        logger.debug("Could not capture cookies in login hook: %s", cookie_exc)
            except Exception as exc:
                logger.error("Login hook error: %s", exc)
            return page

    # ---- Crawl -------------------------------------------------------------
    async with AsyncWebCrawler(config=browser_config) as crawler:
        if args.login_url and args.username and args.password:
            crawler.crawler_strategy.set_hook("on_page_context_created", _login_hook)

        async for result in await crawler.arun(args.url, config=run_config):
            if not result.success:
                stats = getattr(result, "crawl_stats", None) or {}
                # If a PDF URL somehow entered the BFS queue, rescue it
                if args.pdf and result.url.lower().endswith(".pdf"):
                    logger.debug("Rescued PDF URL from failed crawl: %s", result.url)
                    pdf_urls.add(result.url)
                else:
                    logger.warning(
                        "Failed: %s — %s (attempts=%s, resolved_by=%s)",
                        result.url,
                        result.error_message,
                        stats.get("attempts", "?"),
                        stats.get("resolved_by", "none"),
                    )
                continue
            if args.skip_url and any(s in result.url for s in args.skip_url):
                logger.debug("Skipping %s (matches --skip-url filter)", result.url)
                continue
            text = (result.markdown.fit_markdown if result.markdown else "") or ""
            if text.strip():
                pages.append((result.url, text))
                logger.info("[html] %s (%d chars)", result.url, len(text))

            # ---- Collect PDF / docx links from this page ----------------
            if (args.pdf or args.docx) and result.links:
                for link in result.links.get("internal", []) + result.links.get("external", []):
                    href = link.get("href", "")
                    if args.pdf and href.lower().endswith(".pdf") and href not in pdf_urls:
                        pdf_urls.add(href)
                    if args.docx and href.lower().endswith(".docx") and href not in docx_urls:
                        docx_urls.add(href)

        # ---- Locate the Playwright browser context for in-session file downloads
        _playwright_ctx = None
        if pdf_urls or docx_urls:
            try:
                strategy = getattr(crawler, "crawler_strategy", None)
                bm = getattr(strategy, "browser_manager", None)
                _playwright_ctx = (
                    getattr(bm, "default_context", None)
                    or getattr(strategy, "browser_context", None)
                    or getattr(strategy, "context", None)
                    or getattr(strategy, "_context", None)
                )
                if _playwright_ctx is not None:
                    logger.info("Using Playwright browser context for file downloads")
                else:
                    logger.warning(
                        "Could not locate Playwright browser context — falling back to httpx"
                    )
            except Exception as exc:
                logger.warning("Could not locate browser context: %s — falling back to httpx", exc)

        # ---- Download and extract PDFs (inside browser session) ---------------
        if args.pdf and pdf_urls:
            logger.info("Downloading %d linked PDF(s)", len(pdf_urls))
            for pdf_url in sorted(pdf_urls):
                if args.skip_url and any(s in pdf_url for s in args.skip_url):
                    logger.debug("Skipping PDF %s (matches --skip-url filter)", pdf_url)
                    continue
                logger.info("[pdf ] %s", pdf_url)
                pdf_bytes = await _fetch_file_playwright(pdf_url, _playwright_ctx) \
                    if _playwright_ctx else \
                    await _fetch_pdf(pdf_url, session_cookies=_browser_cookies)
                if pdf_bytes is None:
                    continue
                try:
                    text = _read_pdf_bytes(pdf_bytes, columns=args.pdf_columns)
                except Exception as exc:
                    logger.warning("PDF extraction failed for %s: %s", pdf_url, exc)
                    continue
                if text.strip():
                    pages.append((pdf_url, text))
                    logger.info("[pdf ] %s (%d chars)", pdf_url, len(text))

        # ---- Download and extract Word documents (inside browser session) -----
        if args.docx and docx_urls:
            logger.info("Downloading %d linked Word document(s)", len(docx_urls))
            for docx_url in sorted(docx_urls):
                if args.skip_url and any(s in docx_url for s in args.skip_url):
                    logger.debug("Skipping docx %s (matches --skip-url filter)", docx_url)
                    continue
                logger.info("[docx] %s", docx_url)
                docx_bytes = await _fetch_file_playwright(docx_url, _playwright_ctx) \
                    if _playwright_ctx else \
                    await _fetch_docx(docx_url, session_cookies=_browser_cookies)
                if docx_bytes is None:
                    continue
                try:
                    text = _read_docx_bytes(docx_bytes)
                except Exception as exc:
                    logger.warning("Docx extraction failed for %s: %s", docx_url, exc)
                    continue
                if text.strip():
                    pages.append((docx_url, text))
                    logger.info("[docx] %s (%d chars)", docx_url, len(text))

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
    parser.add_argument("--test-run", action="store_true",
                        help="Write extracted text to data/test_run_web_<timestamp>.txt "
                             "instead of ingesting into ChromaDB")
    parser.add_argument("--retries", type=int, default=0,
                        help="Retry rounds when anti-bot blocking is detected (default: 0)")
    parser.add_argument("--proxy", action="append", default=[],
                        metavar="URL",
                        help="Proxy server URL (repeatable). Each retry round tries direct "
                             "first then escalates through the list in order.")
    parser.add_argument("--stealth", action="store_true",
                        help="Enable Playwright stealth mode and magic popup handling")
    parser.add_argument("--skip-url", action="append", default=[],
                        metavar="SUBSTR",
                        help="Skip pages whose URL contains this substring (repeatable)")
    parser.add_argument("--pdf", action="store_true",
                        help="Download and ingest PDF files linked from crawled pages")
    parser.add_argument("--pdf-columns", type=int, default=1,
                        metavar="N",
                        help="Number of columns for PDF OCR layout (default: 1)")
    parser.add_argument("--docx", action="store_true",
                        help="Download and ingest Word (.docx) files linked from crawled pages")
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
    if not args.dry_run and not args.test_run:
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

    # Deduplicate by URL — BFS can surface the same page from multiple parent links
    seen_urls: set[str] = set()
    deduped: list[tuple[str, str]] = []
    for page_url, text in pages:
        if page_url not in seen_urls:
            seen_urls.add(page_url)
            deduped.append((page_url, text))
    if len(deduped) < len(pages):
        logger.info(
            "Deduplicated %d → %d page(s) (%d duplicate URL(s) removed)",
            len(pages), len(deduped), len(pages) - len(deduped),
        )
    pages = deduped

    logger.info("Crawl complete — %d page(s) fetched", len(pages))

    if args.dry_run:
        for page_url, text in pages:
            chunks = _chunk_text(text, args.chunk_size, args.chunk_overlap)
            print(f"  [html] {page_url}  →  {len(chunks)} chunk(s)")
        print(f"\nTotal pages: {len(pages)}")
        return
    if args.test_run:
        import datetime
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = _REPO_ROOT / "data" / f"test_run_web_{ts}.txt"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8") as fh:
            for page_url, text in pages:
                chunks = _chunk_text(text, args.chunk_size, args.chunk_overlap)
                fh.write(f"{'='*80}\n")
                fh.write(f"SOURCE: {page_url}\n")
                fh.write(f"CHUNKS: {len(chunks)}\n")
                fh.write(f"{'='*80}\n")
                fh.write(text)
                fh.write("\n\n")
        logger.info("Test run complete — wrote %d page(s) to %s", len(pages), out_path)
        return
    # ------------------------------------------------------------------
    # Ingest into ChromaDB
    # ------------------------------------------------------------------

    # Build set of already-ingested source URLs so we can skip them (avoids
    # redundant re-embedding on repeat runs without --clear).
    already_ingested: set[str] = set()
    if not args.clear:
        try:
            result = retriever._collection.get(include=["metadatas"])
            for meta in result.get("metadatas") or []:
                if meta and "source" in meta:
                    already_ingested.add(meta["source"])
            if already_ingested:
                logger.info(
                    "%d source(s) already in collection — will skip unchanged URLs",
                    len(already_ingested),
                )
        except Exception as exc:
            logger.debug("Could not fetch existing sources from ChromaDB: %s", exc)

    total_chunks = 0
    skipped = 0
    for page_url, text in pages:
        if page_url in already_ingested:
            logger.debug("Skipping already-ingested %s", page_url)
            skipped += 1
            continue
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
        "Ingestion complete — %d chunk(s) added, %d page(s) skipped (already ingested) "
        "(collection total: %d)",
        total_chunks,
        skipped,
        retriever.document_count,
    )


if __name__ == "__main__":
    main()
