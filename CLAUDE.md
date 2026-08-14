# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

AstroLlama is a local astronomical AI assistant: a FastAPI web client backed by a llama.cpp inference server and a custom MCP (Model Context Protocol) server exposing astronomy tools (SIMBAD, AAVSO, Telescopius observation planning, telescope/camera control via Alpaca/INDI, plate solving via ASTAP, constellation charts, ChromaDB-backed RAG, web/news/arXiv search). Three services run via Docker Compose: `llama` (8081), `mcp` (8000), `app` (8080).

## Running

```powershell
.\start.ps1              # docker compose up -d --build (all 3 services)
.\run_llama.ps1           # llama.cpp container only
.\run_mcp.ps1              # MCP server container only
.\run_client.ps1           # FastAPI app container only
.\stop.ps1                 # docker compose down --remove-orphans
.\restart.ps1               # down + up -d --build
docker compose logs -f      # tail all service logs
```

App served at `http://127.0.0.1:8080`. Config lives in `.env` (copy from `.env.example`); `app/config.py` (`pydantic-settings`) defines all settings and defaults.

For local (non-Docker) iteration, the app and MCP server can also run directly with the venv at `.venv`:
```powershell
python -m uvicorn app.main:app --reload --port 8080
python -m mcp_server.server --http 8000
```

## Tests

```powershell
pytest                              # full suite (see pytest.ini: testpaths=tests, asyncio_mode=auto)
pytest tests/test_simbad_search.py  # single file
pytest -k test_name                 # single test by name
pytest -m "not live"                # skip tests hitting real SIMBAD network calls (marker: live)
```

Tests import the repo via an absolute `sys.path.insert(0, r"c:\Projects\AstroLlama")` at the top of each test file (see `tests/test_mcp_tools_regression.py`) — this is a Windows-path-hardcoded convention already in use; follow it for new test files in this repo rather than relative imports.

## Architecture

### Three-service split

- **`app/`** — FastAPI web client (port 8080). Routers in `app/routers/` (`chat`, `conversations`, `debug`, `files`, `highlight`, `tools`), services in `app/services/` (`llm.py` talks to llama.cpp, `mcp_client.py` talks to the MCP server, `retriever.py` is the ChromaDB RAG layer, `tool_orchestrator.py` is the core chat/tool-call loop, `auth.py` is optional Entra ID validation). Static single-page UI is served straight from `static/`.
- **`mcp_server/`** — standalone MCP server (`mcp_server/server.py`), runnable over stdio or `--http PORT` (streamable-http transport, mounted at `/mcp`). Astronomical data access logic lives in `mcp_server/data_sources/`; tool surface (what the LLM can call) lives in `mcp_server/tools/`.
- **`common/`** — shared code used by both `app` and `mcp_server` containers (mounted into both): `valves_store.py` (SQLite-backed tool config overrides), `alpaca_device_cache.py`, `indi_inventory.py`.
- **llama.cpp** — runs in its own container (CUDA image), no AstroLlama code involved.

### Tool system: OpenWebUI-compatible `Tools` classes

Every file directly under `mcp_server/tools/*.py` (excluding `__init__.py`, `openwebui_adapter.py`, and the `untested/` subfolder) is auto-discovered by `mcp_server/server.py` and must define a class named `Tools` with plain async/sync methods. This is intentionally compatible with OpenWebUI's tool format, so tools can be dropped in from/exported to OpenWebUI unmodified:

- `mcp_server/tools/openwebui_adapter.py` introspects each `Tools` method's signature + Sphinx-style docstring (`:param x:`, `:returns:`) to build the MCP JSON-schema tool definition, and installs runtime shims for OpenWebUI-only imports (`open_webui.models.*`, `open_webui.routers.*`) so unmodified OpenWebUI tool code can still `import` successfully.
- A `Tools` class may declare a nested `class Valves(BaseModel)` (pydantic) for configuration; instance defaults usually read from `os.environ`, and persisted overrides come from `common/valves_store.py` (SQLite at `data/tool_valves.sqlite3`), applied by `_instantiate_tools_class` in `server.py`.
- Tool modules are hot-reloaded: `server.py` fingerprints tool files by mtime + the valves DB and rebuilds the registry when either changes — no server restart needed when editing a tool.
- To add a new tool: drop a `mcp_server/tools/your_tool.py` with a `Tools` class and documented methods (Sphinx-style `:param:`/`:returns:` docstrings drive the schema and description shown to the LLM). Put the actual data-access/API logic in `mcp_server/data_sources/` and have the tool method call into it (see `telescopius_tool.py` + `data_sources/telescopius.py` as the reference pair). Tools that are experimental/unported live under `mcp_server/tools/untested/` and are excluded from discovery.

### Chat / tool-call loop (`app/services/tool_orchestrator.py`)

`run_chat()` is the heart of the app: it streams SSE events (`token`, `tool_start`, `tool_result`, `tool_error`, `tool_download`, `tool_image`, `done`, `error`) to the chat router. Notable, non-obvious behavior:

- **Deterministic fast-paths** run before any LLM call for a handful of high-frequency intents (lat/long lookups, AAVSO finder charts, telescope registration, Alpaca plate-solve/slew/capture commands) — these regex-match the user's literal text and call the MCP tool directly, bypassing the LLM tool-selection step entirely for latency/reliability.
- The system prompt is dynamically extended per-request with a tool-use policy block, ASR (speech-to-text) disambiguation hints, and RAG context — see `_build_context_suffix` in `app/routers/chat.py` for observer location/time injection and `run_chat`'s prompt assembly for the rest. `app/config.py`'s `default_system_prompt` itself bakes in hard tool-routing rules (e.g. never answer planet-visibility questions from training knowledge — always call a Telescopius tool).
- Tool results over `_LARGE_RESULT_THRESHOLD` (10,000 chars) are written to `data/downloads/` and only a preview + download link is sent to the LLM, to avoid blowing the context window.
- ASR disambiguation (`_disambiguate_tool_args`) fuzzy-corrects likely speech-to-text mistranscriptions in tool arguments using recent conversation context (e.g. "seafood variables" → "Cepheid variables") — this exists because the UI supports voice input.
- Duplicate tool calls and repeated-output loop truncation are both detected and short-circuited mid-conversation (see `called_tool_names` tracking and `_strip_repeated_tail`).
- History trimming (`_trim_messages`) uses a rough 3.5 chars/token estimate, not a real tokenizer.

### Config

`app/config.py` (`Settings`, pydantic-settings) is the single source of truth for app-level config, loaded from `.env`. `mcp_server` tools generally read their own config from environment variables directly (see each tool's `Valves` class) plus persisted overrides in `data/tool_valves.sqlite3` — there is no shared settings object between `app` and `mcp_server`.

### RAG ingestion

`scripts/ingest.py` (local files: txt/md/csv/pdf/docx, with optional OCR via Tesseract) and `scripts/web_ingest.py` (Crawl4AI-based headless crawling, with login/stealth/proxy/PDF support) both populate the same ChromaDB collection at `data/chromadb` that `app/services/retriever.py` queries at chat time. Both support `--test-run` to extract/chunk without writing to the DB.

### Docker specifics

`docker/Dockerfile.python-services` is shared by both the `app` and `mcp` Compose services. It builds INDI 2.x client libraries from source and installs `pyindi-client` via a pinned git commit (not PyPI) because Debian's packaged INDI is too old for telescope control; it also installs the ASTAP plate-solver from a bundled `.deb` in `astap/`, and pre-installs CPU-only torch before `requirements.txt` to avoid pulling in ~2.5GB of unused CUDA wheels for `sentence-transformers`. `app` and `mcp` mount `./common` and `./mcp_server` live (not baked into the image) for hot-reload during development; only `app/` and `static/` are live-mounted into `app`, while `mcp_server/` is live-mounted into both.
