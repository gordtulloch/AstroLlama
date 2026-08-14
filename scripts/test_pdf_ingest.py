"""
PDF ingestion test script — extracts text and images from a PDF and writes an HTML report.

Shows page-by-page: element types detected, images extracted, and Chonkie chunks.
Useful for tuning ingest parameters before committing to ChromaDB.

Usage:
    python scripts/test_pdf_ingest.py --pdf "C:/path/to/book.pdf"
    python scripts/test_pdf_ingest.py --pdf "C:/path/to/book.pdf" --pages 1-30
    python scripts/test_pdf_ingest.py --pdf "C:/path/to/book.pdf" --pages 1-30 --chunker semantic
    python scripts/test_pdf_ingest.py --pdf "C:/path/to/book.pdf" --no-layout   # skip unstructured, use fitz only

Output HTML is written to data/ingest_test/<pdf_stem>.html
Images are saved to data/ingest_test/<pdf_stem>/images/
"""
from __future__ import annotations

import argparse
import html
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)
for _noisy in ("httpx", "httpcore", "hpack", "playwright", "asyncio",
               "unstructured", "detectron2", "timm"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

_REPO_ROOT = Path(__file__).parent.parent

# Import helpers from ingest.py (same package)
from scripts.ingest import (
    _chunk_with_chonkie,
    _crop_figure_fitz,
    _ensure_marker_models,
    _extract_embedded_images,
    _extract_layout_elements_unstructured,
    _extract_pages_to_temp,
    _is_scanned_pdf,
    _make_chunker,
    _ocr_pdf_page,
    _read_pdf,
    _read_pdf_marker,
)


# ---------------------------------------------------------------------------
# HTML generation
# ---------------------------------------------------------------------------

_CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
    background: #090d1a;
    color: #c8d0e0;
    font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
    font-size: 14px;
    line-height: 1.6;
    padding: 24px;
}
a { color: #4d9fff; }
h1 { color: #88aaff; font-size: 22px; margin-bottom: 4px; }
.subtitle { color: #5570a0; font-size: 13px; margin-bottom: 20px; }
.summary-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
    gap: 12px;
    margin-bottom: 24px;
}
.stat-card {
    background: #111827;
    border: 1px solid #1e2d4a;
    border-radius: 8px;
    padding: 14px;
    text-align: center;
}
.stat-value { font-size: 28px; font-weight: 700; color: #88aaff; }
.stat-label { font-size: 11px; color: #5570a0; text-transform: uppercase; letter-spacing: .05em; margin-top: 2px; }
details {
    background: #0d1425;
    border: 1px solid #1e2d4a;
    border-radius: 8px;
    margin: 8px 0;
    overflow: hidden;
}
details[open] { border-color: #2a4080; }
summary {
    padding: 12px 16px;
    cursor: pointer;
    font-weight: 600;
    color: #88aaff;
    user-select: none;
    list-style: none;
    display: flex;
    align-items: center;
    gap: 8px;
}
summary::-webkit-details-marker { display: none; }
summary::before { content: '▶'; font-size: 10px; transition: transform .15s; color: #4466aa; }
details[open] summary::before { transform: rotate(90deg); }
summary:hover { background: #10182e; }
.page-badges { margin-left: auto; display: flex; gap: 6px; }
.badge {
    font-size: 11px;
    font-weight: 600;
    padding: 2px 8px;
    border-radius: 10px;
    background: #1e2d4a;
    color: #88aaff;
}
.badge.green { background: #0d2a1a; color: #44cc88; }
.badge.orange { background: #2a1a0d; color: #ffaa44; }
.page-content { padding: 16px; }

/* Images */
.images-grid { display: flex; flex-wrap: wrap; gap: 12px; margin-bottom: 16px; }
.figure-card {
    background: #0a1520;
    border: 1px solid #1a4a2a;
    border-radius: 8px;
    padding: 8px;
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 6px;
    max-width: 320px;
}
.figure-card img {
    max-width: 300px;
    max-height: 220px;
    object-fit: contain;
    border-radius: 4px;
    display: block;
}
.figure-caption { font-size: 11px; color: #44cc88; }

/* Elements */
.elements-section { margin-bottom: 14px; }
.elements-section h3 { font-size: 12px; color: #4466aa; text-transform: uppercase; letter-spacing: .08em; margin-bottom: 8px; }
.element-row {
    background: #111827;
    border-left: 3px solid #1e2d4a;
    border-radius: 0 4px 4px 0;
    padding: 8px 12px;
    margin: 4px 0;
    font-size: 13px;
}
.element-row.Title { border-color: #4466cc; }
.element-row.NarrativeText { border-color: #336633; }
.element-row.Image { border-color: #226644; }
.element-row.Table { border-color: #886622; }
.element-row.Header { border-color: #664488; }
.el-type {
    display: inline-block;
    font-size: 10px;
    font-weight: 700;
    padding: 1px 6px;
    border-radius: 3px;
    margin-right: 8px;
    text-transform: uppercase;
    letter-spacing: .04em;
}
.el-type.Title { background: #1a2a5a; color: #88aaff; }
.el-type.NarrativeText { background: #1a2a1a; color: #66cc66; }
.el-type.Image { background: #0d2a1a; color: #44cc88; }
.el-type.Table { background: #2a1a0d; color: #ffaa44; }
.el-type.Header { background: #2a1a3a; color: #cc88ff; }
.el-type.other { background: #1a1a2a; color: #888899; }

/* Chunks */
.chunks-section { }
.chunks-section h3 { font-size: 12px; color: #4466aa; text-transform: uppercase; letter-spacing: .08em; margin-bottom: 8px; }
.chunk-card {
    background: #111827;
    border: 1px solid #1e2d4a;
    border-radius: 6px;
    padding: 12px 14px;
    margin: 6px 0;
}
.chunk-meta { font-size: 11px; color: #4466aa; margin-bottom: 6px; }
.chunk-text { white-space: pre-wrap; word-break: break-word; }
.no-content { color: #334466; font-style: italic; font-size: 12px; }
"""

_ELEMENT_TYPES_WITH_CLASS = {"Title", "NarrativeText", "Image", "Table", "Header"}


def _el_type_class(el_type: str) -> str:
    return el_type if el_type in _ELEMENT_TYPES_WITH_CLASS else "other"


def _render_page_html(
    page_num: int,
    elements: list[dict],
    chunks: list[str],
    image_paths: list[Path],
    images_rel_dir: Path,
) -> str:
    """Return HTML for one page section (<details> block)."""
    n_imgs = len(image_paths)
    n_chunks = len(chunks)

    badges = f'<span class="badge">{n_chunks} chunk{"s" if n_chunks != 1 else ""}</span>'
    if n_imgs:
        badges += f'<span class="badge green">{n_imgs} fig{"s" if n_imgs != 1 else ""}</span>'

    lines = [
        f'<details open>',
        f'<summary>Page {page_num}'
        f'<span class="page-badges">{badges}</span>'
        f'</summary>',
        f'<div class="page-content">',
    ]

    # Images row
    if image_paths:
        lines.append('<div class="images-grid">')
        for img_path in image_paths:
            rel = img_path.relative_to(images_rel_dir.parent)
            lines.append('<div class="figure-card">')
            lines.append(
                f'<a href="{rel}" target="_blank">'
                f'<img src="{rel}" loading="lazy" alt="Figure page {page_num}">'
                f'</a>'
            )
            lines.append(f'<span class="figure-caption">{rel.name}</span>')
            lines.append('</div>')
        lines.append('</div>')

    # Elements (from unstructured layout analysis)
    if elements:
        lines.append('<div class="elements-section"><h3>Layout elements</h3>')
        for el in elements:
            el_type = el.get("type", "Unknown")
            css = _el_type_class(el_type)
            text_preview = html.escape(el.get("text", "")[:200])
            lines.append(
                f'<div class="element-row {css}">'
                f'<span class="el-type {css}">{html.escape(el_type)}</span>'
                f'{text_preview}'
                f'</div>'
            )
        lines.append('</div>')

    # Chunks
    if chunks:
        lines.append('<div class="chunks-section"><h3>Chunks</h3>')
        for i, chunk in enumerate(chunks):
            token_est = len(chunk) // 4
            lines.append(
                f'<div class="chunk-card">'
                f'<div class="chunk-meta">Chunk {i+1} · ~{token_est} tokens · {len(chunk)} chars</div>'
                f'<div class="chunk-text">{html.escape(chunk)}</div>'
                f'</div>'
            )
        lines.append('</div>')
    else:
        lines.append('<p class="no-content">No text chunks extracted for this page.</p>')

    lines.append('</div></details>')
    return "\n".join(lines)


def _build_html(
    pdf_path: Path,
    page_range: tuple[int, int],
    page_sections: list[str],
    total_chunks: int,
    total_images: int,
    chunker_label: str,
    used_layout: bool,
) -> str:
    """Assemble the full HTML document."""
    title = html.escape(pdf_path.name)
    p_start, p_end = page_range
    page_label = f"Pages {p_start}–{p_end}" if p_start != p_end else f"Page {p_start}"
    method = "unstructured hi_res" if used_layout else "pdfplumber + fitz"

    stats_html = f"""
<div class="summary-grid">
  <div class="stat-card"><div class="stat-value">{p_end - p_start + 1}</div><div class="stat-label">Pages processed</div></div>
  <div class="stat-card"><div class="stat-value">{total_chunks}</div><div class="stat-label">Chunks</div></div>
  <div class="stat-card"><div class="stat-value">{total_images}</div><div class="stat-label">Images extracted</div></div>
</div>
"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Ingest Test: {title}</title>
<style>
{_CSS}
</style>
</head>
<body>
<h1>Ingest Test Report</h1>
<p class="subtitle">
  <strong>{title}</strong> · {page_label} ·
  Chunker: <code>{chunker_label}</code> · Extraction: <code>{method}</code>
</p>
{stats_html}
{"".join(page_sections)}
</body>
</html>"""


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_page_range(s: str | None, total: int) -> tuple[int, int]:
    if not s:
        return (1, total)
    if "-" in s:
        parts = s.split("-", 1)
        start = int(parts[0]) if parts[0].strip() else 1
        end = int(parts[1]) if parts[1].strip() else total
    else:
        start = end = int(s.strip())
    return (max(1, start), min(total, end))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Process a PDF and output an HTML report of extracted text and images."
    )
    parser.add_argument("--pdf", required=True, type=Path, help="Path to the PDF file")
    parser.add_argument(
        "--pages",
        default=None,
        help='Page range to process, e.g. "1-30" or "5" (default: all pages)',
    )
    parser.add_argument(
        "--chunker",
        choices=["semantic", "sentence"],
        default="semantic",
        help="Chunking strategy: semantic (default) or sentence",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=512,
        help="Chunk size in tokens (sentence/semantic) or chars (char) — default 512",
    )
    parser.add_argument(
        "--chunk-overlap",
        type=int,
        default=64,
        help="Overlap in tokens or chars (default: 64)",
    )
    parser.add_argument(
        "--no-marker",
        action="store_true",
        help="Skip Marker; use pdfplumber + fitz only (works outside the ingest container)",
    )
    parser.add_argument(
        "--no-layout",
        action="store_true",
        help="Skip unstructured layout analysis; use fitz embedded-image extraction only",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output HTML path (default: data/ingest_test/<pdf_stem>.html)",
    )
    args = parser.parse_args()

    pdf_path = args.pdf.resolve()
    if not pdf_path.exists():
        logger.error("PDF not found: %s", pdf_path)
        sys.exit(1)

    # Output paths
    out_base = _REPO_ROOT / "data" / "ingest_test"
    images_dir = out_base / pdf_path.stem / "images"
    images_dir.mkdir(parents=True, exist_ok=True)
    html_path = args.output or (out_base / f"{pdf_path.stem}.html")
    html_path.parent.mkdir(parents=True, exist_ok=True)

    # Page count
    import fitz as _fitz
    _doc = _fitz.open(str(pdf_path))
    total_pages = len(_doc)
    _doc.close()

    page_start, page_end = _parse_page_range(args.pages, total_pages)
    logger.info("Processing %s — pages %d to %d", pdf_path.name, page_start, page_end)

    # Scanned detection
    is_scanned = _is_scanned_pdf(pdf_path)
    use_marker = not args.no_marker and not is_scanned
    use_layout = not args.no_layout and (is_scanned or not use_marker)

    if is_scanned:
        logger.info("Detected as scanned PDF — will use unstructured layout analysis")
    elif use_marker:
        logger.info("Text-based PDF — using Marker")
    else:
        logger.info("Using pdfplumber + fitz (no Marker, no layout analysis)")

    # Build chunker — chonkie is mandatory; exit if not available
    chunker = _make_chunker(args.chunker, args.chunk_size, args.chunk_overlap)
    chunker_label = f"{args.chunker} ({args.chunk_size} tokens)"

    # -----------------------------------------------------------------------
    # Extract content
    # -----------------------------------------------------------------------

    # page_data: {page_num: {"text": str, "image_paths": [Path], "elements": [dict]}}
    page_data: dict[int, dict] = {}

    # --- Path A: Marker (text-based PDFs) ---
    if use_marker:
        try:
            # Extract page range to temp for efficiency
            import fitz as _fitz2
            doc_tmp = _fitz2.open(str(pdf_path))
            n_pages = len(doc_tmp)
            doc_tmp.close()
            p_end_actual = min(page_end, n_pages)
            if page_start > 1 or p_end_actual < n_pages:
                tmp_path = _extract_pages_to_temp(pdf_path, page_start, p_end_actual)
            else:
                tmp_path = pdf_path

            marker_text, marker_images = _read_pdf_marker(tmp_path, images_dir)

            if tmp_path != pdf_path:
                tmp_path.unlink(missing_ok=True)

            # Marker gives us one big markdown string; present it as page 0 in the report
            img_paths = [Path(p) for imgs in marker_images.values() for p in imgs if Path(p).exists()]
            page_data[0] = {
                "text": marker_text,
                "image_paths": img_paths,
                "elements": [{"type": "Marker output", "text": ""}],
            }
            logger.info("Marker complete — %d chars, %d image(s)", len(marker_text), len(img_paths))
        except ImportError:
            logger.warning("marker-pdf not installed — falling back to pdfplumber")
            use_marker = False
            use_layout = False
        except Exception as exc:
            logger.warning("Marker failed: %s — falling back to pdfplumber", exc)
            use_marker = False
            use_layout = False

    # --- Path B: unstructured layout (scanned PDFs) ---
    if not use_marker and use_layout:
        try:
            layout_pages = _extract_layout_elements_unstructured(
                pdf_path, images_dir, page_start=page_start, page_end=page_end
            )
            for pnum, pdata in layout_pages.items():
                img_paths = [Path(p) for p in pdata.get("image_paths", []) if Path(p).exists()]
                elements = [{"type": t, "text": ""} for t in pdata.get("element_types", [])]
                page_data[pnum] = {
                    "text": pdata.get("text", ""),
                    "image_paths": img_paths,
                    "elements": elements,
                }
            logger.info("Layout analysis complete — %d page(s) with content", len(page_data))
        except ImportError:
            logger.warning("unstructured not installed — falling back to pdfplumber")
            use_layout = False
        except Exception as exc:
            logger.warning("Layout analysis failed: %s — falling back", exc)
            use_layout = False

    # --- Path C: pdfplumber + fitz fallback ---
    if not use_marker and not use_layout:
        import pdfplumber

        emb_images = _extract_embedded_images(pdf_path, images_dir)
        logger.info("Embedded image extraction complete")

        with pdfplumber.open(pdf_path) as pdf_doc:
            for page_num in range(page_start, page_end + 1):
                if page_num > len(pdf_doc.pages):
                    break
                page = pdf_doc.pages[page_num - 1]
                text = page.extract_text() or ""
                img_paths = [Path(p) for p in emb_images.get(page_num, []) if Path(p).exists()]
                page_data[page_num] = {
                    "text": text,
                    "image_paths": img_paths,
                    "elements": [],
                }

    # -----------------------------------------------------------------------
    # Chunk and render HTML sections
    # -----------------------------------------------------------------------
    page_sections: list[str] = []
    total_chunks = 0
    total_images = 0

    for page_num in sorted(page_data.keys()):
        pdata = page_data[page_num]
        text = pdata.get("text", "")
        img_paths = pdata.get("image_paths", [])
        elements = pdata.get("elements", [])

        if text.strip():
            chunks = _chunk_with_chonkie(text, chunker)
        else:
            chunks = []

        total_chunks += len(chunks)
        total_images += len(img_paths)

        page_sections.append(
            _render_page_html(page_num, elements, chunks, img_paths, out_base)
        )
        logger.info(
            "Page %d — %d chunk(s), %d image(s)", page_num, len(chunks), len(img_paths)
        )

    # -----------------------------------------------------------------------
    # Write HTML
    # -----------------------------------------------------------------------
    doc_html = _build_html(
        pdf_path,
        (page_start, page_end),
        page_sections,
        total_chunks,
        total_images,
        chunker_label,
        use_layout,
    )
    html_path.write_text(doc_html, encoding="utf-8")

    logger.info(
        "Report written → %s  (%d pages, %d chunks, %d images)",
        html_path,
        len(page_data),
        total_chunks,
        total_images,
    )
    logger.info("Open: file:///%s", str(html_path).replace("\\", "/"))


if __name__ == "__main__":
    main()
