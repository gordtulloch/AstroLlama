# AstroLlama

![AstroLlama screenshot](static/readme%20screenshot.png)

A local astronomical AI assistant built on [llama.cpp](https://github.com/ggerganov/llama.cpp). Features a FastAPI/web UI front-end, a Model Context Protocol (MCP) server with astronomical tools (SIMBAD lookups, constellation and AAVSO charts, astroquery integration), ChromaDB-backed RAG from local documents, local conversation persistence, and optional Microsoft Entra ID authentication.

The runtime stack now uses Docker Compose for all required servers.

## Architecture

| Component | Default port | Script |
|-----------|-------------|--------|
| llama.cpp inference server | 8081 | `run_llama.ps1` |
| MCP astronomical-tools server | 8000 | `run_mcp.ps1` |
| FastAPI web client | 8080 | `run_client.ps1` |

## Requirements

- **Docker Desktop** (or Docker Engine + Compose plugin)
- **NVIDIA Container Toolkit** + recent NVIDIA drivers (for CUDA/GPU inference)
- **PowerShell 7+ (`pwsh`)** for the helper scripts
- A **GGUF model** file in `ai/` (default: `Llama-3.2-1B.Q8_0.gguf`)

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/your-org/AstroLlama.git
cd AstroLlama
```

### 2. Download a GGUF model

Place a compatible GGUF model file in the `ai/` directory (example files are already listed there), **or** set `MODEL_PATH` in `.env` to its full path.

Tested models: `Llama-3.2-1B.Q8_0.gguf`, `Qwen2.5-3B-Instruct-Q8_0.gguf`, `mistral-7b-instruct-v0.2.Q3_K_M.gguf`.

### 3. Configure the environment

```powershell
Copy-Item .env.example .env
```

Edit `.env` and set at minimum:

| Variable | Description |
|----------|-------------|
| `HF_TOKEN` | Hugging Face token — needed for RAG embeddings ([get one free](https://huggingface.co/settings/tokens)) |

Optional Docker runtime overrides (used by `docker-compose.yml`):

| Variable | Default | Description |
|----------|---------|-------------|
| `LLAMA_CPP_IMAGE` | `ghcr.io/ggerganov/llama.cpp:server-cuda` | CUDA-enabled llama.cpp server image |
| `LLAMA_MODEL_FILE` | `Llama-3.2-1B.Q8_0.gguf` | Model file name under `./ai` |
| `LLAMA_CTX_SIZE` | `8192` | llama.cpp context window |
| `LLAMA_NGL` | `99` | Number of GPU layers |
| `LLAMA_PORT` | `8081` | Host port mapped to llama service |
| `MCP_PORT` | `8000` | Host port mapped to MCP service |
| `APP_PORT` | `8080` | Host port mapped to FastAPI service |

All other settings have working defaults. See `.env.example` for application config reference.

### 4. Verify Docker GPU access (recommended)

```powershell
docker run --rm --gpus all nvidia/cuda:12.4.1-base-ubuntu22.04 nvidia-smi
```

## Running

### All-in-one (recommended)

Starts llama.cpp (CUDA container), the MCP server, and the web client via Docker Compose:

```powershell
.\start.ps1
```

Optional flags:

```powershell
.\start.ps1 -LlamaPort 8082 -McpPort 8001 -ClientPort 9090
```

Then open **http://127.0.0.1:8080** in your browser.

Equivalent raw Docker command:

```powershell
docker compose up -d --build
```

### Individual components

```powershell
.\run_llama.ps1          # llama.cpp CUDA container (port 8081)
.\run_mcp.ps1            # MCP container (port 8000)
.\run_client.ps1         # FastAPI container (port 8080)
```

Tail logs:

```powershell
docker compose logs -f
```

### Stop / restart

```powershell
.\stop.ps1       # docker compose down --remove-orphans
.\restart.ps1    # docker compose down + up -d --build
```

## RAG — Indexing local documents

The `data/documents/` directory is the default source for the ChromaDB vector store. Supported file types: `.txt`, `.md`, `.csv`, `.pdf`, `.docx`.

```powershell
# Index the default documents folder
python scripts/ingest.py --source data/documents

# Index a single file
python scripts/ingest.py --source path/to/file.pdf

# Clear the collection and re-index
python scripts/ingest.py --source data/documents --clear

# OCR images embedded in PDF pages (requires Tesseract + pymupdf)
python scripts/ingest.py --source data/documents --ocr

# Two-column OCR layout (e.g. newsletters)
python scripts/ingest.py --source data/documents --ocr --columns 2

# Test extraction without writing to ChromaDB — output goes to data/test_run_ingest_<timestamp>.txt
python scripts/ingest.py --source data/documents --test-run
```

### `ingest.py` options

| Flag | Default | Description |
|------|---------|-------------|
| `--source PATH` | `data/documents` | File or directory to ingest |
| `--chunk-size N` | `500` | Characters per chunk |
| `--chunk-overlap N` | `50` | Overlap between chunks |
| `--clear` | off | Wipe the collection before ingesting |
| `--ocr` | off | OCR images embedded in PDF pages |
| `--columns N` | `1` | Vertical column splits per PDF page for OCR |
| `--test` | off | Write extracted PDF text to `data/documents/txt/` |
| `--test-run` | off | Extract and chunk without writing to ChromaDB; output saved to `data/test_run_ingest_<timestamp>.txt` |

## RAG — Web crawling

`web_ingest.py` uses [Crawl4AI](https://docs.crawl4ai.com/) for headless-browser crawling. It handles JavaScript-rendered pages, single-page apps, and paywalled sites. After installing dependencies run the one-time browser setup:

```powershell
pip install crawl4ai
crawl4ai-setup   # downloads Playwright Chromium binaries (~150 MB)
```

### Basic usage

```powershell
# Crawl a public site (depth 3, no login)
python scripts/web_ingest.py --url https://example.com --depth 2

# Limit pages and add a polite delay
python scripts/web_ingest.py --url https://example.com --depth 2 --max-pages 100 --delay 1.0

# Authenticate before crawling (WordPress default field names)
python scripts/web_ingest.py --url https://members.example.com `
    --login-url https://members.example.com/wp-login.php `
    --username myuser --password mypassword

# Also download and ingest linked PDFs (with OCR for image pages)
python scripts/web_ingest.py --url https://example.com --pdf --pdf-columns 2

# Exclude URL patterns (repeatable)
python scripts/web_ingest.py --url https://example.com `
    --skip-url /wp-admin --skip-url /tag/ --skip-url /author/

# Anti-bot: stealth mode + retries + proxy escalation
python scripts/web_ingest.py --url https://protected.example.com `
    --stealth --retries 2 `
    --proxy http://user:pass@datacenter.example.com:8080 `
    --proxy http://user:pass@residential.example.com:9090

# Test extraction without writing to ChromaDB
python scripts/web_ingest.py --url https://example.com --test-run
```

### `web_ingest.py` options

| Flag | Default | Description |
|------|---------|-------------|
| `--url URL` | *(required)* | Start URL to crawl |
| `--depth N` | `3` | Maximum crawl depth |
| `--delay S` | `0.5` | Seconds to wait after each page load |
| `--max-pages N` | `0` (unlimited) | Maximum pages to crawl |
| `--chunk-size N` | `500` | Characters per chunk |
| `--chunk-overlap N` | `50` | Overlap between chunks |
| `--clear` | off | Wipe the ChromaDB collection before ingesting |
| `--skip-url SUBSTR` | *(none)* | Exclude any URL containing this substring (repeatable) |
| `--pdf` | off | Download and ingest PDF files linked from crawled pages |
| `--pdf-columns N` | `1` | Column splits per PDF page for OCR (use `2` for newsletters) |
| `--stealth` | off | Enable Playwright stealth mode and magic popup handling |
| `--retries N` | `0` | Retry rounds when anti-bot blocking is detected |
| `--proxy URL` | *(none)* | Proxy to escalate to after direct attempt fails (repeatable; cheapest first) |
| `--login-url URL` | *(none)* | URL of the login page |
| `--username STR` | *(none)* | Username for login |
| `--password STR` | *(none)* | Password for login |
| `--login-user-field NAME` | `log` | `name` attribute of the username input (WordPress default) |
| `--login-pass-field NAME` | `pwd` | `name` attribute of the password input (WordPress default) |
| `--dry-run` | off | Print page/chunk counts without writing to ChromaDB |
| `--test-run` | off | Crawl and extract without writing to ChromaDB; output saved to `data/test_run_web_<timestamp>.txt` |

RAG is enabled by default (`RAG_ENABLED=true`). Set `RAG_ENABLED=false` in `.env` to disable it.

## MCP astronomical tools

The MCP server exposes tools that the AI can call automatically:

- **SIMBAD object lookup** — resolve names and retrieve object data
- **Astroquery** — access CDS, VizieR, NED, and other archives
- **Constellation maps** — generate star-field charts for any constellation or object
- **AAVSO finder charts** — variable-star comparison charts
- **Variable star comparison stars** — retrieve comparison star sequences

MCP tool use is enabled by default (`MCP_ENABLED=true`). Set `MCP_ENABLED=false` in `.env` to disable it.

## Microsoft Entra ID authentication (optional)

AstroLlama supports protecting the web UI with Microsoft Entra ID (formerly Azure AD). To enable it:

1. Create two Entra app registrations — one for the SPA front-end, one for the API.
2. Fill in the corresponding variables in `.env`:

```ini
ENTRA_AUTH_ENABLED=true
ENTRA_TENANT_ID=<your-tenant-id>
ENTRA_SPA_CLIENT_ID=<spa-app-client-id>
ENTRA_API_CLIENT_ID=<api-app-client-id>
ENTRA_API_SCOPE=api://<ENTRA_API_CLIENT_ID>/access_as_user
ENTRA_REDIRECT_URI=http://127.0.0.1:8080
```

See the comments in `.env.example` for where to find each value in the Azure portal.

## Configuration reference

All settings can be set in `.env`. Key options:

| Variable | Default | Description |
|----------|---------|-------------|
| `LLAMA_SERVER_URL` | `http://127.0.0.1:8081` | llama.cpp server URL |
| `MCP_SERVER_URL` | `http://localhost:8000/mcp` | MCP server endpoint |
| `APP_HOST` | `127.0.0.1` | Host the web client binds to |
| `APP_PORT` | `8080` | Port the web client listens on |
| `DEFAULT_MAX_TOKENS` | `1024` | Maximum tokens per response |
| `DEFAULT_CONTEXT_SIZE` | `4096` | Context window size |
| `RAG_ENABLED` | `true` | Enable ChromaDB retrieval-augmented generation |
| `RAG_TOP_K` | `3` | Number of document chunks to retrieve |
| `EMBEDDING_MODEL` | `all-MiniLM-L6-v2` | Sentence-transformers embedding model |
| `MCP_ENABLED` | `true` | Enable MCP tool calls |
| `HF_TOKEN` | *(empty)* | Hugging Face API token for embeddings |
| `ENTRA_AUTH_ENABLED` | `false` | Enable Microsoft Entra ID authentication |

## Project structure

```
ai/              GGUF model files and llama.cpp binaries (ai/bin/)
app/             FastAPI application (routers, services, models)
data/
  chromadb/      ChromaDB vector store
  documents/     Source documents for RAG ingestion
  conversations/ Persisted conversation history
mcp_server/      MCP server and astronomical data-source modules
scripts/         Document and web ingestion utilities
static/          Web UI (HTML, CSS, JavaScript)
tests/           Test suite
```

## License

See [LICENSE](LICENSE).

