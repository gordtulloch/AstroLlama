from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.models.chat import ChatSettings
from app.services.tool_orchestrator import (
    _extract_alpaca_current_plate_solve_request,
    _extract_alpaca_capture_request,
    _extract_alpaca_slew_plate_solve_request,
    run_chat,
)


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


def test_extract_alpaca_slew_plate_solve_request_parses_direct_command():
    args = _extract_alpaca_slew_plate_solve_request("Slew to M45 and platesolve")

    assert args == {
        "object_name": "M45",
    }


def test_extract_alpaca_current_plate_solve_request_parses_direct_command():
    args = _extract_alpaca_current_plate_solve_request(
        "Plate solve at current location and return current coordinates"
    )

    assert args == {}


def test_extract_alpaca_current_plate_solve_request_parses_companion_phrases():
    assert _extract_alpaca_current_plate_solve_request("platesolve here") == {}
    assert _extract_alpaca_current_plate_solve_request("verify current pointing") == {}


class FailingLLMClientPlateSolve:
    async def chat_stream(self, *args, **kwargs):
        raise AssertionError("LLM should not be called for deterministic Alpaca slew+plate-solve requests")
        yield  # pragma: no cover


class FakeMCPClientPlateSolve:
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
                    "name": "alpaca_slew_and_plate_solve",
                    "description": "direct telescope slew + plate solve tool",
                    "parameters": {"type": "object", "properties": {}},
                },
            },
        ]
        self.calls: list[tuple[str, dict]] = []

    async def call_tool(self, name: str, arguments: dict):
        self.calls.append((name, arguments))
        return [SimpleNamespace(text="Plate solved: True")]


class FakeMCPClientPlateSolveStaleTools:
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
            }
        ]
        self.calls: list[tuple[str, dict]] = []
        self.reconnect_calls = 0

    async def reconnect(self):
        self.reconnect_calls += 1
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
                    "name": "alpaca_slew_and_plate_solve",
                    "description": "direct telescope slew + plate solve tool",
                    "parameters": {"type": "object", "properties": {}},
                },
            },
        ]

    async def call_tool(self, name: str, arguments: dict):
        self.calls.append((name, arguments))
        return [SimpleNamespace(text="Plate solved: True")]


class FailingLLMClientCurrentPlateSolve:
    async def chat_stream(self, *args, **kwargs):
        raise AssertionError("LLM should not be called for deterministic current-location plate-solve requests")
        yield  # pragma: no cover


class FakeMCPClientCurrentPlateSolve:
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
                    "name": "alpaca_plate_solve_current_position",
                    "description": "plate solve at current pointing",
                    "parameters": {"type": "object", "properties": {}},
                },
            },
        ]
        self.calls: list[tuple[str, dict]] = []

    async def call_tool(self, name: str, arguments: dict):
        self.calls.append((name, arguments))
        return [SimpleNamespace(text="Current coordinates: RA=3.0h DEC=24.0deg\nPlate solved: True")]


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


@pytest.mark.asyncio
async def test_run_chat_uses_alpaca_slew_plate_solve_fast_path_and_skips_orchestrate():
    history = [
        {"role": "system", "content": "You are AstroLlama."},
        {"role": "user", "content": "Slew to M45 and platesolve"},
    ]
    settings = ChatSettings()
    llm_client = FailingLLMClientPlateSolve()
    mcp_client = FakeMCPClientPlateSolve()

    events = [event async for event in run_chat(history, settings, llm_client, mcp_client, retriever=None)]

    assert mcp_client.calls == [
        (
            "alpaca_slew_and_plate_solve",
            {
                "object_name": "M45",
            },
        )
    ]
    assert [event["type"] for event in events] == ["tool_start", "tool_result", "token", "done"]
    assert events[0]["name"] == "alpaca_slew_and_plate_solve"
    assert "Plate solved: True" in events[1]["result"]
    assert "Plate solved: True" in events[2]["text"]


@pytest.mark.asyncio
async def test_run_chat_refreshes_stale_tools_for_alpaca_slew_plate_solve_fast_path():
    history = [
        {"role": "system", "content": "You are AstroLlama."},
        {"role": "user", "content": "Slew to m45 and platesolve"},
    ]
    settings = ChatSettings()
    llm_client = FailingLLMClientPlateSolve()
    mcp_client = FakeMCPClientPlateSolveStaleTools()

    events = [event async for event in run_chat(history, settings, llm_client, mcp_client, retriever=None)]

    assert mcp_client.reconnect_calls == 1
    assert mcp_client.calls == [
        (
            "alpaca_slew_and_plate_solve",
            {
                "object_name": "m45",
            },
        )
    ]
    assert [event["type"] for event in events] == ["tool_start", "tool_result", "token", "done"]
    assert events[0]["name"] == "alpaca_slew_and_plate_solve"


@pytest.mark.asyncio
async def test_run_chat_uses_alpaca_current_plate_solve_fast_path_and_skips_orchestrate():
    history = [
        {"role": "system", "content": "You are AstroLlama."},
        {"role": "user", "content": "Plate solve at current location and return current coordinates"},
    ]
    settings = ChatSettings()
    llm_client = FailingLLMClientCurrentPlateSolve()
    mcp_client = FakeMCPClientCurrentPlateSolve()

    events = [event async for event in run_chat(history, settings, llm_client, mcp_client, retriever=None)]

    assert mcp_client.calls == [
        (
            "alpaca_plate_solve_current_position",
            {},
        )
    ]
    assert [event["type"] for event in events] == ["tool_start", "tool_result", "token", "done"]
    assert events[0]["name"] == "alpaca_plate_solve_current_position"
    assert "Current coordinates" in events[1]["result"]
    assert "Current coordinates" in events[2]["text"]