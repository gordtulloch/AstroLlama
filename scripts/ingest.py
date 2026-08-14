"""
Document ingestion script for ChromaDB RAG.

Usage:
    python scripts/ingest.py --source data/documents
    python scripts/ingest.py --source path/to/file.pdf
    python scripts/ingest.py --source path/to/folder --chunk-size 512 --chunk-overlap 64
    python scripts/ingest.py --clear                         # wipe collection and re-index
    python scripts/ingest.py --source path/to/folder --test  # write extracted PDF text to data/documents/txt/
    python scripts/ingest.py --source file.pdf --extract-images              # save figures to data/images/
    python scripts/ingest.py --source file.pdf --extract-images --layout     # use unstructured hi_res for scanned PDFs
    python scripts/ingest.py --source file.pdf --chunker sentence            # Chonkie sentence-aware chunking (default)
    python scripts/ingest.py --source file.pdf --chunker semantic            # Chonkie embedding-based chunking
    python scripts/ingest.py --source file.pdf --chunker char                # legacy fixed-char chunking

Supported file types: .txt, .md, .csv, .pdf, .docx

Chunker modes (--chunker):
    sentence  Chonkie SentenceChunker  — respects sentence boundaries (default)
    semantic  Chonkie SemanticChunker  — groups by embedding similarity; best for dense technical prose
    char      Legacy fixed-character splits with overlap

When --chunker is sentence or semantic, --chunk-size is in *tokens* (default 512).
When --chunker is char, --chunk-size is in *characters* (default 500).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import settings
from app.services.retriever import Retriever

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

for _noisy in ("httpx", "httpcore", "hpack", "playwright", "asyncio"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

_REPO_ROOT = Path(__file__).parent.parent
_SUPPORTED = {".txt", ".md", ".csv", ".pdf", ".docx"}
_SCANNED_THRESHOLD_CHARS_PER_PAGE = 50  # avg chars/page below this → treat as scanned


# ---------------------------------------------------------------------------
# Chunking
# ---------------------------------------------------------------------------

def _chunk_text(text: str, chunk_size: int, overlap: int) -> list[str]:
    """Legacy fixed-character chunking with overlap."""
    chunks: list[str] = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunks.append(text[start:end].strip())
        start += chunk_size - overlap
    return [c for c in chunks if c]


def _make_chunker(chunker_type: str, chunk_size: int, chunk_overlap: int):
    """Return a Chonkie chunker instance. Exits the process if chonkie is not installed."""
    try:
        if chunker_type == "semantic":
            from chonkie import SemanticChunker
            return SemanticChunker(
                embedding_model=settings.embedding_model,
                chunk_size=chunk_size,
                similarity_threshold=0.5,
            )
        elif chunker_type == "sentence":
            from chonkie import SentenceChunker
            return SentenceChunker(
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
                min_sentences_per_chunk=1,
            )
        else:
            logger.error("Unknown chunker type: %r. Choose: semantic, sentence", chunker_type)
            sys.exit(1)
    except ImportError as exc:
        logger.error(
            "chonkie is required but not installed in this environment: %s\n"
            "Add 'chonkie[semantic]' to requirements.txt and rebuild the Docker image.",
            exc,
        )
        sys.exit(1)


def _chunk_with_chonkie(text: str, chunker) -> list[str]:
    """Chunk text using a Chonkie chunker. Returns list of chunk strings."""
    if not text.strip():
        return []
    chunks = chunker.chunk(text)
    return [c.text for c in chunks if c.text and c.text.strip()]


# ---------------------------------------------------------------------------
# PDF: Marker conversion (text-based PDFs)
# ---------------------------------------------------------------------------

_marker_models = None  # loaded once per process; expensive to initialise


def _ensure_marker_models():
    """Load Marker model dict; exits the process if marker-pdf is not installed."""
    global _marker_models
    if _marker_models is not None:
        return _marker_models
    try:
        from marker.models import create_model_dict
        logger.info("Loading Marker models (first run will download ~1–2 GB)…")
        _marker_models = create_model_dict()
        logger.info("Marker models ready")
        return _marker_models
    except ImportError as exc:
        logger.error(
            "marker-pdf is required for text-PDF ingestion but is not installed: %s\n"
            "Install via requirements-ingest.txt in the ingest Docker container, "
            "or pass --no-marker to fall back to pdfplumber.",
            exc,
        )
        sys.exit(1)


def _read_pdf_marker(
    path: Path,
    images_dir: Path | None = None,
) -> tuple[str, dict[int, list[str]]]:
    """
    Convert a text-based PDF to clean Markdown using Marker.
    Returns (markdown_text, {page_num: [saved_image_paths]}).
    If images_dir is given, figures embedded by Marker are saved there.
    """
    from marker.converters.pdf import PdfConverter

    models = _ensure_marker_models()
    converter = PdfConverter(artifact_dict=models)
    rendered = converter(str(path))

    markdown_text: str = getattr(rendered, "markdown", "") or ""
    raw_images: dict = getattr(rendered, "images", {}) or {}
    page_images: dict[int, list[str]] = {}

    if images_dir and raw_images:
        images_dir.mkdir(parents=True, exist_ok=True)
        for idx, (key, img) in enumerate(raw_images.items()):
            # Key format varies across Marker versions; try to parse page number.
            page_num = 0
            for part in str(key).replace("\\", "/").split("/"):
                clean = part.split("_")[0]
                if clean.isdigit():
                    page_num = int(clean)
                    break

            ext = Path(str(key)).suffix or ".png"
            img_path = images_dir / f"p{page_num}_m{idx}{ext}"
            try:
                if hasattr(img, "save"):  # PIL Image
                    img.save(str(img_path))
                elif isinstance(img, bytes):
                    img_path.write_bytes(img)
                else:
                    continue
                page_images.setdefault(page_num, []).append(str(img_path))
            except Exception as exc:
                logger.debug("Could not save Marker image %s: %s", key, exc)

    return markdown_text, page_images


# ---------------------------------------------------------------------------
# PDF: scanned-page detection
# ---------------------------------------------------------------------------

def _is_scanned_pdf(path: Path) -> bool:
    """Return True if the PDF appears to be a scanned document (image-only pages)."""
    try:
        import pdfplumber
        with pdfplumber.open(path) as pdf:
            if not pdf.pages:
                return False
            sample = min(5, len(pdf.pages))
            total_chars = sum(
                len(pdf.pages[i].extract_text() or "") for i in range(sample)
            )
            return (total_chars / sample) < _SCANNED_THRESHOLD_CHARS_PER_PAGE
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Image extraction — embedded XOBJECTs (non-scanned PDFs)
# ---------------------------------------------------------------------------

def _extract_embedded_images(path: Path, images_dir: Path) -> dict[int, list[str]]:
    """
    Extract image XOBJECTs embedded in a PDF using PyMuPDF.
    Returns {page_num (1-indexed): [absolute_image_paths]}.
    Skips tiny images (< 64×64 px) that are likely decorative bullets/icons.
    """
    import fitz

    images_dir.mkdir(parents=True, exist_ok=True)
    page_images: dict[int, list[str]] = {}
    doc = fitz.open(stream=path.read_bytes(), filetype="pdf")

    for page_idx in range(len(doc)):
        page_num = page_idx + 1
        image_list = doc[page_idx].get_images(full=True)
        saved: list[str] = []

        for img_idx, img_info in enumerate(image_list):
            xref = img_info[0]
            try:
                base_image = doc.extract_image(xref)
                w, h = base_image["width"], base_image["height"]
                if w < 64 or h < 64:
                    continue
                ext = base_image["ext"]
                img_bytes = base_image["image"]
                img_path = images_dir / f"p{page_num}_i{img_idx}.{ext}"
                img_path.write_bytes(img_bytes)
                saved.append(str(img_path))
            except Exception as exc:
                logger.debug("Could not extract image xref %d on page %d: %s", xref, page_num, exc)

        if saved:
            page_images[page_num] = saved

    doc.close()
    return page_images


# ---------------------------------------------------------------------------
# Image extraction — layout analysis via unstructured (scanned / mixed PDFs)
# ---------------------------------------------------------------------------

def _extract_pages_to_temp(src: Path, start: int, end: int) -> Path:
    """Extract a page range from a PDF into a temp file. Returns temp path."""
    import fitz
    import tempfile

    doc = fitz.open(stream=src.read_bytes(), filetype="pdf")
    sub = fitz.open()
    actual_end = min(end, len(doc)) - 1
    sub.insert_pdf(doc, from_page=start - 1, to_page=actual_end)
    tmp = Path(tempfile.mktemp(suffix=".pdf"))
    sub.save(str(tmp))
    sub.close()
    doc.close()
    return tmp


def _crop_figure_fitz(
    doc,  # fitz.Document
    page_num: int,
    pixel_points: list[tuple[float, float]],
    output_path: Path,
    hi_res_dpi: float = 200.0,
) -> bool:
    """
    Render a bounding box region from a PDF page using PyMuPDF and save as PNG.
    pixel_points come from unstructured PixelSpace at hi_res_dpi DPI.
    Returns True if the image was saved successfully.
    """
    import fitz

    try:
        page = doc[page_num - 1]
        scale = 72.0 / hi_res_dpi  # pixels → PDF points
        x0 = min(p[0] for p in pixel_points) * scale
        y0 = min(p[1] for p in pixel_points) * scale
        x1 = max(p[0] for p in pixel_points) * scale
        y1 = max(p[1] for p in pixel_points) * scale
        rect = fitz.Rect(x0, y0, x1, y1)
        if rect.is_empty or rect.is_infinite or rect.width < 10 or rect.height < 10:
            return False
        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2), clip=rect)
        pix.save(str(output_path))
        return True
    except Exception as exc:
        logger.debug("Crop failed for page %d: %s", page_num, exc)
        return False


def _extract_layout_elements_unstructured(
    path: Path,
    images_dir: Path,
    page_start: int = 1,
    page_end: int | None = None,
) -> dict[int, dict]:
    """
    Use unstructured hi_res layout analysis to detect text elements and figure regions.
    Figures are cropped from the rendered page using PyMuPDF.

    Returns {page_num: {"text": str, "image_paths": [str], "element_types": [str]}}
    Page numbers in the returned dict are always relative to the *original* PDF.
    """
    import fitz
    from unstructured.partition.pdf import partition_pdf

    images_dir.mkdir(parents=True, exist_ok=True)

    # Limit to page range for speed on large PDFs
    doc_full = fitz.open(stream=path.read_bytes(), filetype="pdf")
    total_pages = len(doc_full)
    p_end = min(page_end or total_pages, total_pages)
    use_temp = page_start > 1 or p_end < total_pages
    target_path = _extract_pages_to_temp(path, page_start, p_end) if use_temp else path

    try:
        elements = partition_pdf(
            filename=str(target_path),
            strategy="hi_res",
            extract_images_in_pdf=False,  # we crop ourselves for predictable naming
            infer_table_structure=False,
        )
    finally:
        if use_temp:
            target_path.unlink(missing_ok=True)

    # Re-open full doc for cropping (avoids re-rendering the temp page range)
    pages: dict[int, dict] = {}
    fig_counters: dict[int, int] = {}

    for el in elements:
        raw_page = getattr(el.metadata, "page_number", None) or 1
        page_num = raw_page + (page_start - 1) if use_temp else raw_page

        if page_num not in pages:
            pages[page_num] = {"text_parts": [], "image_paths": [], "element_types": []}

        el_type = type(el).__name__
        pages[page_num]["element_types"].append(el_type)

        if el_type in ("Image", "Figure"):
            coords = getattr(el.metadata, "coordinates", None)
            if coords and hasattr(coords, "points") and coords.points:
                idx = fig_counters.get(page_num, 0)
                fig_counters[page_num] = idx + 1
                out_path = images_dir / f"p{page_num}_fig{idx}.png"
                if _crop_figure_fitz(doc_full, page_num, list(coords.points), out_path):
                    pages[page_num]["image_paths"].append(str(out_path))
        elif el.text and el.text.strip():
            pages[page_num]["text_parts"].append(el.text)

    doc_full.close()

    for pdata in pages.values():
        pdata["text"] = "\n\n".join(pdata["text_parts"])

    return pages


# ---------------------------------------------------------------------------
# OCR helpers
# ---------------------------------------------------------------------------

def _read_pdf_pdftotext(path: Path) -> str:
    """Extract text via poppler's pdftotext — handles malformed PDFs that pdfminer/MuPDF reject."""
    import subprocess

    result = subprocess.run(
        ["pdftotext", "-layout", str(path), "-"],
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.decode("utf-8", errors="replace").strip())
    return result.stdout.decode("utf-8", errors="replace")


def _read_pdf_poppler_ocr(path: Path, columns: int = 1) -> str:
    """Render pages via pdftoppm and OCR — final fallback for PDFs MuPDF can't open."""
    import io
    import subprocess
    import tempfile

    import pytesseract
    from PIL import Image

    if sys.platform == "win32":
        _win_default = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
        if Path(_win_default).exists():
            pytesseract.pytesseract.tesseract_cmd = _win_default

    with tempfile.TemporaryDirectory() as tmpdir:
        result = subprocess.run(
            ["pdftoppm", "-png", "-r", "200", str(path), f"{tmpdir}/page"],
            capture_output=True,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.decode("utf-8", errors="replace").strip())

        page_files = sorted(Path(tmpdir).glob("page-*.png"))
        pages: list[str] = []
        for page_path in page_files:
            img = Image.open(page_path)
            if columns <= 1:
                text = pytesseract.image_to_string(img).strip()
            else:
                width, height = img.size
                strip_w = width // columns
                parts = []
                for col in range(columns):
                    left = col * strip_w
                    right = width if col == columns - 1 else left + strip_w
                    parts.append(pytesseract.image_to_string(img.crop((left, 0, right, height))))
                text = "\n\n".join(t for t in parts if t.strip())
            if text:
                pages.append(text)
        return "\n".join(pages)


def _read_pdf_fitz_ocr(path: Path, columns: int = 1) -> str:
    """Render every page via MuPDF (fitz) and OCR — last resort for PDFs that
    pdfplumber/PDFium reject (e.g. old-format or structurally unusual scans)."""
    import io

    import fitz
    import pytesseract
    from PIL import Image

    if sys.platform == "win32":
        _win_default = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
        if Path(_win_default).exists():
            pytesseract.pytesseract.tesseract_cmd = _win_default

    # Stream avoids MuPDF path-based type detection that rejects some valid PDFs
    doc = fitz.open(stream=path.read_bytes(), filetype="pdf")
    pages: list[str] = []
    for page in doc:
        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
        img = Image.open(io.BytesIO(pix.tobytes("png")))
        if columns <= 1:
            text = pytesseract.image_to_string(img).strip()
        else:
            width, height = img.size
            strip_w = width // columns
            parts = []
            for col in range(columns):
                left = col * strip_w
                right = width if col == columns - 1 else left + strip_w
                parts.append(pytesseract.image_to_string(img.crop((left, 0, right, height))))
            text = "\n\n".join(t for t in parts if t.strip())
        if text:
            pages.append(text)
    return "\n".join(pages)


def _ocr_pdf_page(path: Path, page_index: int, columns: int = 1) -> str:
    """Render a single PDF page to an image and return OCR'd text."""
    import io

    import fitz
    import pytesseract
    from PIL import Image

    if sys.platform == "win32":
        _win_default = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
        if Path(_win_default).exists():
            pytesseract.pytesseract.tesseract_cmd = _win_default

    doc = fitz.open(stream=path.read_bytes(), filetype="pdf")
    page = doc[page_index]
    pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
    img = Image.open(io.BytesIO(pix.tobytes("png")))

    if columns <= 1:
        return pytesseract.image_to_string(img)

    width, height = img.size
    strip_w = width // columns
    column_texts: list[str] = []
    for col in range(columns):
        left = col * strip_w
        right = width if col == columns - 1 else left + strip_w
        strip = img.crop((left, 0, right, height))
        column_texts.append(pytesseract.image_to_string(strip))
    return "\n\n".join(t for t in column_texts if t.strip())


def _read_pdf(path: Path, ocr: bool = False, columns: int = 1) -> str:
    """Extract text from a PDF using pdfplumber, with optional OCR for image-bearing pages."""
    import pdfplumber

    pages: list[str] = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            if ocr and page.images:
                try:
                    ocr_text = _ocr_pdf_page(path, page.page_number - 1, columns=columns).strip()
                    if ocr_text and ocr_text not in text:
                        text = (text + "\n" + ocr_text).strip()
                except Exception as exc:
                    logger.warning("OCR failed on page %d of %s: %s", page.page_number, path.name, exc)
            if text:
                pages.append(text)
    return "\n".join(pages)


def _read_docx(path: Path) -> str:
    """Extract text from a DOCX file using python-docx."""
    from docx import Document

    doc = Document(str(path))
    return "\n".join(para.text for para in doc.paragraphs if para.text)


# ---------------------------------------------------------------------------
# Stable chunk ID
# ---------------------------------------------------------------------------

def _stable_id(source: str, page_num: int, chunk_index: int) -> str:
    raw = f"{source}::p{page_num}::{chunk_index}"
    return hashlib.md5(raw.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Main ingest function
# ---------------------------------------------------------------------------

def ingest(
    source: Path,
    retriever: Retriever,
    chunk_size: int,
    chunk_overlap: int,
    ocr: bool = False,
    columns: int = 1,
    dump_txt: bool = False,
    extract_images: bool = False,
    chunker_type: str = "semantic",
    layout_analysis: bool = False,
    use_marker: bool = True,
) -> int:
    """Ingest a single file or all supported files in a directory. Returns chunk count."""
    txt_out_dir = _REPO_ROOT / "data" / "documents" / "txt"
    images_base_dir = _REPO_ROOT / "data" / "images"

    files: list[Path] = []
    if source.is_dir():
        for ext in _SUPPORTED:
            files.extend(source.rglob(f"*{ext}"))
    elif source.suffix in _SUPPORTED:
        files = [source]
    else:
        logger.warning("Unsupported file type: %s — skipping", source)
        return 0

    # Build chunker once (shared across all files)
    chunker = None
    if chunker_type != "char":
        chunker = _make_chunker(chunker_type, chunk_size, chunk_overlap)

    total = 0
    for file in files:
        # -------------------------------------------------------------------
        # PDF: decide path (layout analysis vs. normal)
        # -------------------------------------------------------------------
        if file.suffix == ".pdf" and extract_images and layout_analysis:
            is_scanned = _is_scanned_pdf(file)
            if is_scanned:
                logger.info("%s detected as scanned PDF — using unstructured layout analysis", file.name)
            images_dir = images_base_dir / file.stem
            images_dir.mkdir(parents=True, exist_ok=True)

            if is_scanned or layout_analysis:
                try:
                    page_data = _extract_layout_elements_unstructured(file, images_dir)
                    # Chunk each page separately so images stay associated
                    for page_num in sorted(page_data.keys()):
                        pdata = page_data[page_num]
                        text = pdata["text"]
                        img_paths = pdata["image_paths"]
                        if not text.strip() and not img_paths:
                            continue

                        rel_images = [
                            str(Path(p).relative_to(images_base_dir))
                            for p in img_paths
                            if Path(p).exists()
                        ]

                        if text.strip():
                            chunks = _chunk_with_chonkie(text, chunker)
                        else:
                            chunks = []

                        if not chunks and rel_images:
                            # Image-only page: store a placeholder chunk so images are retrievable
                            chunks = [f"[Figure page {page_num} of {file.stem}]"]

                        ids = [_stable_id(str(file), page_num, i) for i in range(len(chunks))]
                        metadatas = [
                            {
                                "source": str(file),
                                "source_title": file.stem,
                                "chunk": i,
                                "page_num": page_num,
                                "has_images": bool(rel_images),
                                "images": json.dumps(rel_images),
                            }
                            for i in range(len(chunks))
                        ]
                        retriever.add_documents(chunks, ids, metadatas)
                        total += len(chunks)

                    logger.info("Ingested %s (layout) → %d page(s)", file.name, len(page_data))
                    continue
                except ImportError:
                    logger.warning("unstructured not installed — falling back to standard extraction for %s", file.name)
                except Exception as exc:
                    logger.warning("Layout analysis failed for %s: %s — falling back", file.name, exc)

        # -------------------------------------------------------------------
        # Path B: Marker — text-based PDF → clean Markdown + images
        # -------------------------------------------------------------------
        if file.suffix == ".pdf" and use_marker:
            images_dir = (images_base_dir / file.stem) if extract_images else None
            try:
                text, marker_image_map = _read_pdf_marker(file, images_dir)
                # Build rel-path image map for metadata
                page_image_map: dict[int, list[str]] = {}
                if images_dir:
                    for pg, paths in marker_image_map.items():
                        page_image_map[pg] = [
                            str(Path(p).relative_to(images_base_dir))
                            for p in paths if Path(p).exists()
                        ]
                    n_imgs = sum(len(v) for v in page_image_map.values())
                    if n_imgs:
                        logger.info("Marker extracted %d image(s) from %s", n_imgs, file.name)

                if dump_txt and text:
                    try:
                        txt_out_dir.mkdir(parents=True, exist_ok=True)
                        out_path = txt_out_dir / (file.stem + ".md")
                        out_path.write_text(text, encoding="utf-8")
                        logger.info("Wrote Marker markdown → %s", out_path.relative_to(_REPO_ROOT))
                    except Exception as exc:
                        logger.warning("Could not write dump for %s: %s", file.name, exc)

                chunks = _chunk_with_chonkie(text, chunker) if text.strip() else []
                if not chunks:
                    logger.warning("Marker produced no chunks for %s", file.name)
                    continue

                # All images stored against page 0 (whole-document; Marker doesn't
                # give reliable per-page boundaries in the text output).
                all_rel_images = [p for imgs in page_image_map.values() for p in imgs]
                ids = [_stable_id(str(file), 0, i) for i in range(len(chunks))]
                metadatas = [
                    {
                        "source": str(file),
                        "source_title": file.stem,
                        "chunk": i,
                        "page_num": 0,
                        "has_images": bool(all_rel_images),
                        "images": json.dumps(all_rel_images),
                    }
                    for i in range(len(chunks))
                ]
                retriever.add_documents(chunks, ids, metadatas)
                logger.info("Ingested %s (Marker) → %d chunk(s)", file.name, len(chunks))
                total += len(chunks)
                continue
            except ImportError:
                logger.warning("Marker not available for %s — falling back to pdfplumber", file.name)
            except Exception as exc:
                logger.warning("Marker failed on %s: %s — falling back to pdfplumber", file.name, exc)

        # -------------------------------------------------------------------
        # Path C: fallback — pdfplumber text + optional fitz image extraction
        # -------------------------------------------------------------------
        try:
            if file.suffix == ".pdf":
                text = _read_pdf(file, ocr=ocr, columns=columns)
            elif file.suffix == ".docx":
                text = _read_docx(file)
            else:
                text = file.read_text(encoding="utf-8", errors="replace")
        except Exception as exc:
            if file.suffix == ".pdf":
                # pdfplumber/pdfminer rejected the file; cascade through poppler then fitz
                try:
                    text = _read_pdf_fitz_ocr(file, columns=columns)
                    if text.strip():
                        logger.info("fitz OCR fallback succeeded for %s", file.name)
                    else:
                        raise RuntimeError("fitz OCR returned no text")
                except Exception as fitz_exc:
                    try:
                        text = _read_pdf_pdftotext(file)
                        if text.strip():
                            logger.info("pdftotext fallback succeeded for %s", file.name)
                        else:
                            raise RuntimeError("pdftotext returned no text")
                    except Exception as poppler_exc:
                        try:
                            text = _read_pdf_poppler_ocr(file, columns=columns)
                            if text.strip():
                                logger.info("pdftoppm OCR fallback succeeded for %s", file.name)
                            else:
                                logger.warning("All fallbacks produced no text for %s — skipping", file.name)
                                continue
                        except Exception as ppocr_exc:
                            logger.warning(
                                "Could not read %s: %s | fitz: %s | pdftotext: %s | pdftoppm: %s",
                                file, exc, fitz_exc, poppler_exc, ppocr_exc,
                            )
                            continue
            else:
                logger.warning("Could not read %s: %s", file, exc)
                continue

        if dump_txt and file.suffix == ".pdf" and text:
            try:
                txt_out_dir.mkdir(parents=True, exist_ok=True)
                out_path = txt_out_dir / (file.stem + ".txt")
                out_path.write_text(text, encoding="utf-8")
                logger.info("Wrote extracted text → %s", out_path.relative_to(_REPO_ROOT))
            except Exception as exc:
                logger.warning("Could not write test output for %s: %s", file.name, exc)

        page_image_map: dict[int, list[str]] = {}
        if extract_images and file.suffix == ".pdf":
            images_dir = images_base_dir / file.stem
            try:
                raw_map = _extract_embedded_images(file, images_dir)
                page_image_map = {
                    pg: [str(Path(p).relative_to(images_base_dir)) for p in paths]
                    for pg, paths in raw_map.items()
                }
                n_images = sum(len(v) for v in page_image_map.values())
                logger.info("Extracted %d embedded image(s) from %s", n_images, file.name)
            except Exception as exc:
                logger.warning("Image extraction failed for %s: %s", file.name, exc)

        chunks = _chunk_with_chonkie(text, chunker) if text.strip() else []
        if not chunks:
            continue

        all_rel_images = [p for imgs in page_image_map.values() for p in imgs]
        ids = [_stable_id(str(file), 0, i) for i in range(len(chunks))]
        metadatas = [
            {
                "source": str(file),
                "source_title": file.stem,
                "chunk": i,
                "page_num": 0,
                "has_images": bool(all_rel_images),
                "images": json.dumps(all_rel_images),
            }
            for i in range(len(chunks))
        ]
        retriever.add_documents(chunks, ids, metadatas)
        logger.info("Ingested %s → %d chunk(s)", file.name, len(chunks))
        total += len(chunks)

    return total


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest documents into ChromaDB")
    parser.add_argument(
        "--source",
        type=Path,
        default=_REPO_ROOT / "data" / "documents",
        help="File or directory to ingest (default: data/documents/)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=512,
        help="Chunk size: tokens for sentence/semantic chunkers, chars for char (default: 512)",
    )
    parser.add_argument(
        "--chunk-overlap",
        type=int,
        default=64,
        help="Overlap between chunks: tokens for sentence, chars for char (default: 64)",
    )
    parser.add_argument(
        "--chunker",
        choices=["semantic", "sentence"],
        default="semantic",
        help="Chunking strategy: semantic (default, embedding-based) or sentence (boundary-aware)",
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Delete all existing documents before ingesting",
    )
    parser.add_argument(
        "--ocr",
        action="store_true",
        help="OCR image-bearing pages in PDFs (requires Tesseract + pymupdf)",
    )
    parser.add_argument(
        "--columns",
        type=int,
        default=1,
        help="Columns per page for OCR (default: 1)",
    )
    parser.add_argument(
        "--extract-images",
        action="store_true",
        help="Extract and save figures from PDFs to data/images/<stem>/",
    )
    parser.add_argument(
        "--no-marker",
        action="store_true",
        help="Skip Marker; use pdfplumber for text extraction (useful outside the ingest container)",
    )
    parser.add_argument(
        "--layout",
        action="store_true",
        help="Use unstructured hi_res layout analysis for figure detection (requires unstructured-inference)",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Write extracted PDF text to data/documents/txt/ for inspection",
    )
    parser.add_argument(
        "--test-run",
        action="store_true",
        help="Extract and chunk without writing to ChromaDB; save to data/test_run_ingest_<timestamp>.txt",
    )
    args = parser.parse_args()

    retriever = None
    if not args.test_run:
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
            client = chromadb.PersistentClient(path=str(_REPO_ROOT / settings.chroma_db_path))
            client.delete_collection(settings.chroma_collection)
            retriever.start()
            logger.info("Collection cleared")
            return

    source = args.source.resolve()
    if not source.exists():
        logger.error("Source path does not exist: %s", source)
        sys.exit(1)

    logger.info("Chunker: %s (chunk_size=%d tokens, overlap=%d tokens)", args.chunker, args.chunk_size, args.chunk_overlap)

    if args.extract_images:
        logger.info("Image extraction enabled → data/images/")
    if args.layout:
        logger.info("Layout analysis enabled (unstructured hi_res)")

    if args.test_run:
        import datetime
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = _REPO_ROOT / "data" / f"test_run_ingest_{ts}.txt"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        files: list[Path] = []
        if source.is_dir():
            for ext in _SUPPORTED:
                files.extend(source.rglob(f"*{ext}"))
        elif source.suffix in _SUPPORTED:
            files = [source]

        chunker = None
        if args.chunker != "char":
            chunker = _make_chunker(args.chunker, args.chunk_size, args.chunk_overlap)

        total_chunks = 0
        with out_path.open("w", encoding="utf-8") as fh:
            for file in files:
                try:
                    if file.suffix == ".pdf":
                        text = _read_pdf(file, ocr=args.ocr, columns=args.columns)
                    elif file.suffix == ".docx":
                        text = _read_docx(file)
                    else:
                        text = file.read_text(encoding="utf-8", errors="replace")
                except Exception as exc:
                    logger.warning("Could not read %s: %s", file, exc)
                    continue
                if text.strip():
                    chunks = _chunk_with_chonkie(text, chunker)
                else:
                    chunks = []
                if not chunks:
                    continue
                fh.write(f"{'='*80}\n")
                fh.write(f"SOURCE: {file}\n")
                fh.write(f"CHUNKS: {len(chunks)}\n")
                fh.write(f"{'='*80}\n")
                fh.write(text)
                fh.write("\n\n")
                total_chunks += len(chunks)
                logger.info("Extracted %s → %d chunk(s)", file.name, len(chunks))
        logger.info("Test run complete — %d chunk(s) from %d file(s) written to %s",
                    total_chunks, len(files), out_path)
        return

    count = ingest(
        source,
        retriever,
        args.chunk_size,
        args.chunk_overlap,
        ocr=args.ocr,
        columns=args.columns,
        dump_txt=args.test,
        extract_images=args.extract_images,
        chunker_type=args.chunker,
        layout_analysis=args.layout,
        use_marker=not args.no_marker,
    )
    logger.info("Done — %d chunk(s) ingested (collection total: %d)", count, retriever.document_count)


if __name__ == "__main__":
    main()
