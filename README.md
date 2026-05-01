# AstroLlama

![AstroLlama screenshot](static/readme%20screenshot.png)

A local astronomical AI assistant built on [llama.cpp](https://github.com/ggerganov/llama.cpp). Features a FastAPI/web UI front-end, a Model Context Protocol (MCP) server with astronomical tools (SIMBAD lookups, constellation and AAVSO charts, astroquery integration), ChromaDB-backed RAG from local documents, local conversation persistence, and optional Microsoft Entra ID authentication.

## Architecture

| Component | Default port | Script |
|-----------|-------------|--------|
| llama.cpp inference server | 8081 | `run_llama.ps1` |
| MCP astronomical-tools server | 8000 | `run_mcp.ps1` |
| FastAPI web client | 8080 | `run_client.ps1` |

## Requirements

- **Python 3.11+** on PATH
- **PowerShell 7+ (`pwsh`)** — required on macOS/Linux; PowerShell 5.1 works on Windows
- **llama.cpp** binaries (`llama-server`) — place them in `ai/bin/` or set `LLAMA_CPP_PATH` in `.env`
- A **GGUF model** file — place it in `ai/` or set `MODEL_PATH` in `.env`
- *(Optional)* NVIDIA CUDA 12.x for GPU-accelerated inference

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/your-org/AstroLlama.git
cd AstroLlama
```

### 2. Obtain llama.cpp binaries

Download a pre-built release from the [llama.cpp releases page](https://github.com/ggerganov/llama.cpp/releases) or build from source, then either:

- Copy the binaries into `ai/bin/`, **or**
- Set the `LLAMA_CPP_PATH` variable in `.env` to the directory containing `llama-server`.

### 3. Download a GGUF model

Place a compatible GGUF model file in the `ai/` directory (example files are already listed there), **or** set `MODEL_PATH` in `.env` to its full path.

Tested models: `Llama-3.2-1B.Q8_0.gguf`, `Qwen2.5-3B-Instruct-Q8_0.gguf`, `mistral-7b-instruct-v0.2.Q3_K_M.gguf`.

### 4. Configure the environment

```powershell
Copy-Item .env.example .env
```

Edit `.env` and set at minimum:

| Variable | Description |
|----------|-------------|
| `LLAMA_CPP_PATH` | Directory containing `llama-server` (if not using `ai/bin/`) |
| `MODEL_PATH` | Full path to the GGUF model file (if not in `ai/`) |
| `HF_TOKEN` | Hugging Face token — needed for RAG embeddings ([get one free](https://huggingface.co/settings/tokens)) |

All other settings have working defaults. See `.env.example` for the full reference.

### 5. Create the Python virtual environment

The run scripts create and populate the virtual environment automatically on first launch. To do it manually:

```powershell
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt
```

## Running

### All-in-one (recommended)

Starts llama-server, the MCP server, and the web client each in a separate terminal window:

```powershell
.\start.ps1
```

Optional flags:

```powershell
.\start.ps1 -LlamaPort 8082 -McpPort 8001 -ClientPort 9090
.\start.ps1 -NoDelay   # skip the brief pause between component launches
```

Then open **http://127.0.0.1:8080** in your browser.

### Individual components

```powershell
.\run_llama.ps1          # llama.cpp inference server (port 8081)
.\run_mcp.ps1            # MCP astronomical-tools server (port 8000)
.\run_client.ps1         # FastAPI web client (port 8080)
.\run_client.ps1 -Reload # hot-reload mode for development
```

### Stop / restart

```powershell
.\stop.ps1       # gracefully stop all components
.\restart.ps1    # stop then restart all components
```

### Linux / macOS

Use the equivalent shell scripts:

```bash
./start.sh
./stop.sh
./restart.sh
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

# Crawl a website and ingest its content (requires crawl4ai — see below)
python scripts/web_ingest.py --url https://example.com --depth 2

# Limit pages, add a polite delay, or authenticate first
python scripts/web_ingest.py --url https://example.com --depth 2 --max-pages 100 --delay 1.0
python scripts/web_ingest.py --url https://members.example.com `
    --login-url https://members.example.com/wp-login.php `
    --username myuser --password mypassword
```

### Web crawling with Crawl4AI

`web_ingest.py` uses [Crawl4AI](https://docs.crawl4ai.com/) for headless-browser crawling. It handles JavaScript-rendered pages, single-page apps, and paywalled sites that Scrapy cannot reach. After installing dependencies, run the one-time browser setup:

```powershell
pip install crawl4ai
crawl4ai-setup   # downloads Playwright Chromium binaries (~150 MB)
```

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

