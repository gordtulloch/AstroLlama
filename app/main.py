from __future__ import annotations

import logging
import logging.config
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.routers import chat as chat_router
from app.routers import conversations as conv_router
from app.routers import files as files_router
from app.routers import highlight as highlight_router
from app.services.auth import EntraTokenValidator
from app.services.llm import LLMClient
from app.services.mcp_client import MCPClient
from app.services.retriever import Retriever

_REPO_ROOT = Path(__file__).parent.parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
# Enable DEBUG for tool/LLM diagnostics
logging.getLogger("app.services.llm").setLevel(logging.DEBUG)
logging.getLogger("app.services.tool_orchestrator").setLevel(logging.DEBUG)
# The MCP SSE library logs its own ERROR + traceback before raising when the
# server is unreachable. We handle that gracefully in MCPClient.start() and
# emit our own WARNING, so suppress the library's internal noise.
logging.getLogger("mcp.client.sse").setLevel(logging.CRITICAL)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

class _StatusFilter(logging.Filter):
    """Drop uvicorn access log lines for /api/status polling."""
    def filter(self, record: logging.LogRecord) -> bool:
        return "/api/status" not in record.getMessage()

logging.getLogger("uvicorn.access").addFilter(_StatusFilter())

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ---- startup ----
    conv_dir = _REPO_ROOT / "data" / "conversations"
    conv_dir.mkdir(parents=True, exist_ok=True)
    app.state.conv_dir = conv_dir

    downloads_dir = _REPO_ROOT / "data" / "downloads"
    downloads_dir.mkdir(parents=True, exist_ok=True)

    llm_client = LLMClient(settings.llama_server_url)
    app.state.llm_client = llm_client
    logger.info("LLM client ready → %s", settings.llama_server_url)

    mcp_client = MCPClient(settings.mcp_server_url)
    app.state.mcp_client = mcp_client
    if settings.mcp_enabled:
        await mcp_client.start()
    else:
        logger.info("MCP tool use disabled via config")

    retriever = Retriever(
        db_path=str(_REPO_ROOT / settings.chroma_db_path),
        collection_name=settings.chroma_collection,
        embedding_model=settings.embedding_model,
        top_k=settings.rag_top_k,
        hf_token=settings.hf_token,
    )
    if settings.rag_enabled:
        retriever.start()
    else:
        logger.info("RAG disabled via config")
    app.state.retriever = retriever

    if settings.entra_auth_enabled:
        if not settings.entra_tenant_id or not settings.entra_api_client_id:
            logger.warning("Entra auth enabled but tenant or API client ID is not configured")
        else:
            app.state.entra_validator = EntraTokenValidator(
                tenant_id=settings.entra_tenant_id,
                api_client_id=settings.entra_api_client_id,
                api_scope=settings.entra_api_scope,
            )
            logger.info("Entra token validation enabled for tenant %s", settings.entra_tenant_id)

    yield

    # ---- shutdown ----
    await mcp_client.stop()
    await llm_client.aclose()
    logger.info("Shutdown complete")


app = FastAPI(title="LocalAI Chat Client", version="0.1.0", lifespan=lifespan)

app.include_router(chat_router.router, prefix="/api")
app.include_router(conv_router.router, prefix="/api")
app.include_router(files_router.router, prefix="/api")
app.include_router(highlight_router.router)

# Serve the static single-page UI
_static_dir = _REPO_ROOT / "static"
if _static_dir.exists():
    app.mount("/", StaticFiles(directory=str(_static_dir), html=True), name="static")
