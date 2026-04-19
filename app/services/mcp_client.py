from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

logger = logging.getLogger(__name__)


def _mcp_to_openai_tool(tool: Any) -> dict[str, Any]:
    """Convert an MCP Tool object to the OpenAI function-tool schema."""
    return {
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description or "",
            "parameters": tool.inputSchema,
        },
    }


class MCPClient:
    """
    Persistent MCP client. Call `start()` once on startup and `stop()` on shutdown.
    Tool calls automatically attempt a reconnect if the session is unavailable.
    """

    def __init__(self, server_url: str) -> None:
        self.server_url = server_url
        self._session: ClientSession | None = None
        self._exit_stack = contextlib.AsyncExitStack()
        self._tools: list[dict[str, Any]] = []
        self._available: bool = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        stack = contextlib.AsyncExitStack()
        try:
            read, write, _ = await stack.enter_async_context(
                streamablehttp_client(self.server_url)
            )
            session = await stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
            self._session = session
            self._exit_stack = stack
            await self._refresh_tools()
            self._available = True
            logger.info("MCP server connected — %d tool(s) available", len(self._tools))
        except (Exception, asyncio.CancelledError, RuntimeError) as exc:
            # CancelledError is BaseException in Python 3.8+, not Exception.
            # RuntimeError can be raised by anyio cancel-scope cleanup when the
            # streamablehttp_client task group tears down on a failed connection.
            self._available = False
            with contextlib.suppress(Exception):
                await stack.aclose()
            logger.warning("MCP server unavailable at startup: %s", exc)

    async def stop(self) -> None:
        await self._exit_stack.aclose()
        self._session = None
        self._available = False

    async def reconnect(self) -> None:
        await self.stop()
        await self.start()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def tools(self) -> list[dict[str, Any]]:
        return list(self._tools)

    @property
    def available(self) -> bool:
        return self._available

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> Any:
        if not self._available or self._session is None:
            logger.info("MCP not connected — attempting reconnect before tool call")
            await self.reconnect()
            if not self._available:
                raise RuntimeError("MCP server is unavailable")

        result = await self._session.call_tool(name, arguments)
        # Return the raw content list; callers decide how to serialize it.
        return result.content

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _refresh_tools(self) -> None:
        assert self._session is not None
        logger.info("MCP → requesting tool list from %s", self.server_url)
        result = await self._session.list_tools()
        self._tools = [_mcp_to_openai_tool(t) for t in result.tools]
        logger.info("MCP ← received %d tool(s): %s",
                    len(self._tools),
                    ", ".join(t["function"]["name"] for t in self._tools) or "(none)")
