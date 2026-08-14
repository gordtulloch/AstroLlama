# AstroLlama RAG Ingestion — Architecture & Implementation Guide

## Background

Before this work the ingestion pipeline had these limitations:

- **Chunking**: Fixed 500-char character splits — no sentence or semantic awareness
- **PDF images**: OCR'd to text only; images were never extracted or saved
- **ChromaDB metadata**: Only `{source, chunk}` — no page numbers, no image references
- **Retriever**: Returned `list[str]` text only — callers never saw metadata
- **Chat**: No image linking to RAG results

The goals of this work were to replace all of the above with semantic chunking, full image extraction and linking, and a dedicated GPU ingestion container capable of handling both text-based and scanned astronomy PDFs.

---

## PDF Processing Decision Tree

```
PDF Input
  │
  ├─ _is_scanned_pdf() → True (avg < 50 chars/page)
  │    └─ unstructured hi_res layout analysis
  │         ├─ Text elements → Chonkie SemanticChunker → ChromaDB
  │         └─ Image/Figure elements → fitz crop → data/images/{stem}/
  │
  └─ Text-based PDF (default)
       └─ Marker (marker-pdf)
            ├─ Clean Markdown → Chonkie SemanticChunker → ChromaDB
            └─ Embedded figures → data/images/{stem}/
                 (fallback: --no-marker → pdfplumber + fitz XOBJECTs)
```

---

## Container Architecture

```
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────────────┐
│  llama           │  │  app             │  │  ingest  [profile: ingest]   │
│  GPU · port 8081 │  │  CPU · port 8080 │  │  GPU · one-shot job          │
│  llama.cpp model │  │  FastAPI UI      │  │  Marker + unstructured       │
└──────────────────┘  └──────────────────┘  │  Chonkie SemanticChunker     │
                               │             └──────────────┬───────────────┘
                       ┌───────▼─────────────────────────────▼──────┐
                       │              ./data/  (shared volume)       │
                       │   chromadb/   images/   documents/          │
                       └─────────────────────────────────────────────┘
```

The `ingest` container uses `profiles: ["ingest"]` so it does **not** start with a normal `docker compose up`. It runs as a one-shot job and exits. Model weights (Marker, unstructured-inference) are cached in the `marker_cache` named Docker volume so they are not re-downloaded on every run.

---

## ChromaDB Metadata Schema

Every stored chunk carries these fields:

| Field          | Type         | Example                                 |
|----------------|--------------|-----------------------------------------|
| `source`       | `str`        | `/app/data/documents/Encyclopedia.pdf`  |
| `source_title` | `str`        | `Encyclopedia`                          |
| `chunk`        | `int`        | `3`                                     |
| `page_num`     | `int`        | `12` (0 = unknown / whole-document)     |
| `has_images`   | `bool`       | `True`                                  |
| `images`       | `str` (JSON) | `'["Encyclopedia/p12_m0.png"]'`         |

`images` is a JSON-encoded list of paths **relative to `data/images/`** — ChromaDB only accepts scalar metadata values. `Retriever.query_with_metadata()` decodes the JSON back to a list on retrieval.

---

## Chat Integration

When a RAG query returns chunks that have associated images:

1. `Retriever.query_with_metadata()` decodes the `images` JSON field.
2. `tool_orchestrator.py` injects `[Figure: /images/...]` references into the LLM system context so the model can reference them in its answer.
3. A `tool_image` SSE event is emitted for each figure URL so the frontend renders it inline alongside the response text.

`data/images/` is mounted as a FastAPI static directory at `/images`, so any saved figure is immediately accessible at `http://localhost:8080/images/<stem>/p3_fig0.png`.

---

## Implementation Status

| Phase | Description | Status |
|---|---|---|
| 1 | Scanned-PDF auto-detection (`_is_scanned_pdf`) | ✅ Implemented |
| 2 | Image extraction from embedded-XOBJECT PDFs (fitz) | ✅ Implemented |
| 3 | unstructured hi_res layout analysis + fitz figure cropping for scanned PDFs | ✅ Implemented |
| 4 | Marker integration for text-based PDFs → clean Markdown + figures | ✅ Implemented |
| 5 | Richer ChromaDB metadata (`page_num`, `images` JSON, `source_title`) | ✅ Implemented |
| 6 | Chonkie `SemanticChunker` — mandatory, exits if missing | ✅ Implemented |
| 7 | `Retriever.query_with_metadata()` | ✅ Implemented |
| 8 | FastAPI `/images` static mount | ✅ Implemented |
| 9 | `tool_image` SSE events emitted from RAG results in `tool_orchestrator.py` | ✅ Implemented |
| 10 | `scripts/test_pdf_ingest.py` HTML validation report | ✅ Implemented |
| 11 | Dedicated `ingest` Docker container (GPU, Marker, model cache volume) | ✅ Implemented |

---

## Packages & Where They Live

| Package | Container | Purpose |
|---|---|---|
| `chonkie[semantic]` | ingest, app, mcp | Semantic chunking |
| `marker-pdf>=1.0.0` | **ingest only** | Text-PDF → Markdown + figure extraction |
| `unstructured[pdf]` | ingest, app, mcp | Scanned-PDF layout detection |
| `unstructured-inference` | ingest, app, mcp | hi_res YOLOX layout model |
| `pymupdf` | ingest, app, mcp | PDF rendering + image extraction / cropping |

`marker-pdf` and its deps (surya, texify, ~2 GB of model weights) are installed only in the ingest container via `requirements-ingest.txt`. The app and mcp containers do not carry them.

---

## Running the Ingest Container

### First-time setup

```powershell
# Build the ingest container (downloads base image + installs all deps)
docker compose --profile ingest build ingest
```

### Test a PDF (HTML visual report — no ChromaDB write)

```powershell
docker compose --profile ingest run --rm ingest `
  python scripts/test_pdf_ingest.py `
  --pdf "/books/Encyclopedia of Astronomy and Astrophysics.pdf" `
  --pages 1-20

# View the report
start data\ingest_test\Encyclopedia of Astronomy and Astrophysics.html
```

### Ingest a single PDF into ChromaDB

```powershell
docker compose --profile ingest run --rm ingest `
  python scripts/ingest.py `
  --source "/books/Encyclopedia of Astronomy and Astrophysics.pdf" `
  --extract-images
```

### Ingest a whole directory

```powershell
docker compose --profile ingest run --rm ingest `
  python scripts/ingest.py `
  --source /books `
  --extract-images `
  --layout
```

### Clear and re-index

```powershell
docker compose --profile ingest run --rm ingest python scripts/ingest.py --clear
```

### Fallback: run without Marker (works in the app container, no GPU required)

```powershell
docker compose exec app python scripts/ingest.py `
  --source /app/data/documents `
  --extract-images `
  --no-marker
```

---

## Configuration (.env)

```dotenv
# Path on the host to mount as /books inside the ingest container
BOOKS_PATH=C:\Users\gordt\Dropbox\Books\Astronomy
```

---

## `scripts/ingest.py` CLI Reference

```
--source PATH         File or directory to ingest (default: data/documents/)
--chunker             semantic (default) | sentence
--chunk-size N        Tokens per chunk (default: 512)
--chunk-overlap N     Token overlap (default: 64)
--extract-images      Save figures to data/images/<stem>/
--layout              Use unstructured hi_res for scanned PDFs
--no-marker           Skip Marker; use pdfplumber instead
--ocr                 Tesseract OCR for image-bearing pages (pdfplumber path only)
--columns N           Columns per page for OCR (default: 1)
--clear               Wipe ChromaDB collection then exit
--test                Write extracted text to data/documents/txt/
--test-run            Dry-run: extract+chunk, write to file, do not touch ChromaDB
```

## `scripts/test_pdf_ingest.py` CLI Reference

```
--pdf PATH            PDF file to process (required)
--pages N-M           Page range, e.g. "1-30" or "5" (default: all)
--chunker             semantic (default) | sentence
--chunk-size N        Tokens per chunk (default: 512)
--chunk-overlap N     Token overlap (default: 64)
--no-marker           Skip Marker; use pdfplumber instead
--no-layout           Skip unstructured; use fitz embedded-image extraction only
--output PATH         HTML output path (default: data/ingest_test/<stem>.html)
```

---

## Model Weight Sizes (approximate)

| Component | Size | Cache location |
|---|---|---|
| Marker (surya + texify) | ~1.5 GB | `marker_cache` named volume |
| unstructured-inference YOLOX | ~400 MB | `marker_cache` named volume |
| Chonkie SemanticChunker (MiniLM) | ~90 MB | `marker_cache` named volume |
| all-MiniLM-L6-v2 (ChromaDB embeddings) | ~90 MB | `marker_cache` named volume |

All model weights are cached in the `marker_cache` named Docker volume (`/root/.cache` inside the container) and survive container removal.

---

## Tuning Tips

- **`--chunk-size`**: 512 tokens (~2000 chars) works well for encyclopedia entries. Raise to 768–1024 for books with long flowing prose.
- **`--pages 1-30`**: Use the test script on a representative subset before ingesting a whole document.
- **Scanned detection threshold**: `_SCANNED_THRESHOLD_CHARS_PER_PAGE = 50` in `ingest.py`. Lower for figure-heavy documents; raise for text-heavy scans.
- **Similarity threshold**: `SemanticChunker(similarity_threshold=0.5)` in `_make_chunker()`. Lower (0.3) produces larger chunks; raise (0.7) for finer splits.
