from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.models.chat import ChatSettings
from app.services.tool_orchestrator import _extract_alpaca_capture_request, run_chat


class FailingLLMClient:
    async def chat_stream(self, *args, **kwargs):
        raise AssertionError("LLM should not be called for deterministic Alpaca capture requests")
        yield  # pragma: no cover


class FakeMCPClient:
    def __init__(self) -> None:
        self.available = True
        self.tools = [
            {
                "type": "function",
                "function": {
                    "name": "orchestrate",
                    "description": "planning tool",
                    "parameters": {"type": "object", "properties": {}},
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "alpaca_slew_and_capture",
                    "description": "direct telescope imaging tool",
                    "parameters": {"type": "object", "properties": {}},
                },
            },
        ]
        self.calls: list[tuple[str, dict]] = []

    async def call_tool(self, name: str, arguments: dict):
        self.calls.append((name, arguments))
        return [SimpleNamespace(text='{"job_id":"job-123","status":"running"}')]


def test_extract_alpaca_capture_request_parses_direct_command():
    args = _extract_alpaca_capture_request("Move the telescope to M45 and take a 10 second exposure")

    assert args == {
        "object_name": "M45",
        "exposure_seconds": 10.0,
        "exposure_count": 1,
        "light_frame": True,
    }


@pytest.mark.asyncio
async def test_run_chat_uses_alpaca_fast_path_and_skips_orchestrate():
    history = [
        {"role": "system", "content": "You are AstroLlama."},
        {"role": "user", "content": "Move the telescope to M45 and take a 10 second exposure"},
    ]
    settings = ChatSettings()
    llm_client = FailingLLMClient()
    mcp_client = FakeMCPClient()

    events = [event async for event in run_chat(history, settings, llm_client, mcp_client, retriever=None)]

    assert mcp_client.calls == [
        (
            "alpaca_slew_and_capture",
            {
                "object_name": "M45",
                "exposure_seconds": 10.0,
                "exposure_count": 1,
                "light_frame": True,
            },
        )
    ]
    assert [event["type"] for event in events] == ["tool_start", "tool_result", "token", "done"]
    assert events[0]["name"] == "alpaca_slew_and_capture"
    assert "job-123" not in events[1]["result"]
    assert "job-123" not in events[2]["text"]
    assert "Ask for the capture status when you want an update." in events[2]["text"]

    hidden_tool_messages = [msg for msg in history if msg.get("role") == "tool"]
    assert len(hidden_tool_messages) == 1
    assert "job-123" in hidden_tool_messages[0]["content"]