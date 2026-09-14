from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from app.models.chat import ChatSettings
from app.services.tool_orchestrator import (
    _extract_alpaca_current_plate_solve_request,
    _extract_alpaca_capture_request,
    _extract_alpaca_slew_plate_solve_request,
    _extract_telescope_registration_request,
    _prepare_llm_tool_content,
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


def test_extract_telescope_registration_request_parses_indi_command():
    args = _extract_telescope_registration_request("Register an INDI telescope at spao-s30.local")

    assert args == {
        "platform": "indi",
        "address": "spao-s30.local",
        "auto_select": True,
    }


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


class FailingLLMClientRegisterTelescope:
    async def chat_stream(self, *args, **kwargs):
        raise AssertionError("LLM should not be called for deterministic telescope registration requests")
        yield  # pragma: no cover


class FakeMCPClientRegisterTelescope:
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
                    "name": "register_telescope",
                    "description": "register telescope profile",
                    "parameters": {"type": "object", "properties": {}},
                },
            },
        ]
        self.calls: list[tuple[str, dict]] = []

    async def call_tool(self, name: str, arguments: dict):
        self.calls.append((name, arguments))
        return [SimpleNamespace(text='{"status":"registered","platform":"indi"}')]


class ScriptedLLMClient:
    def __init__(self, steps: list[dict]) -> None:
        self.steps = steps
        self.call_index = 0

    async def chat_stream(self, messages, tools, **kwargs):
        step = self.steps[self.call_index]
        self.call_index += 1
        inspector = step.get("inspect")
        if inspector:
            inspector(messages, tools)

        if "tool_call" in step:
            tool_call = step["tool_call"]
            yield {
                "choices": [
                    {
                        "delta": {
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "id": tool_call.get("id", f"call_{self.call_index}"),
                                    "function": {
                                        "name": tool_call["name"],
                                        "arguments": json.dumps(tool_call.get("arguments", {})),
                                    },
                                }
                            ]
                        }
                    }
                ]
            }
            return

        for token in step.get("content", []):
            yield {"choices": [{"delta": {"content": token}}]}


class FakeMCPClientWebFallback:
    def __init__(self) -> None:
        self.available = True
        self.tools = [
            {
                "type": "function",
                "function": {
                    "name": "simbad_lookup_object",
                    "description": "direct astronomy lookup",
                    "parameters": {"type": "object", "properties": {}},
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "search_web",
                    "description": "web search fallback",
                    "parameters": {"type": "object", "properties": {}},
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "scrape_website",
                    "description": "retrieve readable page content",
                    "parameters": {"type": "object", "properties": {}},
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "search_news",
                    "description": "news feed search",
                    "parameters": {"type": "object", "properties": {}},
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "search_youtube",
                    "description": "youtube search",
                    "parameters": {"type": "object", "properties": {}},
                },
            },
        ]
        self.calls: list[tuple[str, dict]] = []

    async def call_tool(self, name: str, arguments: dict):
        self.calls.append((name, arguments))
        if name == "simbad_lookup_object":
            return [SimpleNamespace(text="No results found for that object.")]
        if name == "search_web":
            return [SimpleNamespace(text='[{"title":"Betelgeuse","url":"https://example.com/betelgeuse"}]')]
        if name == "scrape_website":
            return [SimpleNamespace(text="# Betelgeuse\n\nBetelgeuse is a red supergiant star in Orion.")]
        if name == "search_news":
            return [SimpleNamespace(text="No articles found")]
        if name == "search_youtube":
            return [SimpleNamespace(text="No videos found")]
        raise AssertionError(f"Unexpected tool call: {name}")


class EmptyRetriever:
    available = True
    document_count = 1

    def query(self, text: str) -> list[str]:
        return []


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


@pytest.mark.asyncio
async def test_run_chat_uses_telescope_registration_fast_path_and_skips_orchestrate():
    history = [
        {"role": "system", "content": "You are AstroLlama."},
        {"role": "user", "content": "Register an INDI telescope at spao-s30.local"},
    ]
    settings = ChatSettings()
    llm_client = FailingLLMClientRegisterTelescope()
    mcp_client = FakeMCPClientRegisterTelescope()

    events = [event async for event in run_chat(history, settings, llm_client, mcp_client, retriever=None)]

    assert mcp_client.calls == [
        (
            "register_telescope",
            {
                "platform": "indi",
                "address": "spao-s30.local",
                "auto_select": True,
            },
        )
    ]
    assert [event["type"] for event in events] == ["tool_start", "tool_result", "token", "done"]
    assert events[0]["name"] == "register_telescope"


@pytest.mark.asyncio
async def test_run_chat_prompts_web_search_after_empty_tool_result():
    history = [
        {"role": "system", "content": "You are AstroLlama."},
        {"role": "user", "content": "Tell me about Betelgeuse"},
    ]
    settings = ChatSettings()
    mcp_client = FakeMCPClientWebFallback()

    def inspect_second_call(messages, tools):
        assert any(tool["function"]["name"] == "search_web" for tool in tools)
        tool_messages = [msg for msg in messages if msg.get("role") == "tool"]
        assert tool_messages
        assert "did not return any useful results" in tool_messages[-1]["content"]

    llm_client = ScriptedLLMClient(
        [
            {"tool_call": {"name": "simbad_lookup_object", "arguments": {"object_name": "Betelgeuse"}}},
            {
                "inspect": inspect_second_call,
                "tool_call": {"name": "search_web", "arguments": {"query": "Betelgeuse star overview"}},
            },
            {"content": ["Betelgeuse is a red supergiant."]},
        ]
    )

    events = [event async for event in run_chat(history, settings, llm_client, mcp_client, retriever=None)]

    assert mcp_client.calls == [
        ("simbad_lookup_object", {"object_name": "Betelgeuse"}),
        ("search_web", {"query": "Betelgeuse star overview"}),
    ]
    assert events[-1]["type"] == "done"


@pytest.mark.asyncio
async def test_run_chat_marks_empty_rag_context_for_web_search_fallback():
    history = [
        {"role": "system", "content": "You are AstroLlama."},
        {"role": "user", "content": "What is the latest on Betelgeuse?"},
    ]
    settings = ChatSettings()
    mcp_client = FakeMCPClientWebFallback()

    def inspect_first_call(messages, tools):
        system_message = messages[0]["content"]
        assert "No local knowledge-base context was injected" in system_message
        assert "search_web" in system_message

    llm_client = ScriptedLLMClient(
        [
            {
                "inspect": inspect_first_call,
                "tool_call": {"name": "search_web", "arguments": {"query": "latest Betelgeuse news"}},
            },
            {"content": ["Here are the latest web findings."]},
        ]
    )

    events = [
        event
        async for event in run_chat(history, settings, llm_client, mcp_client, retriever=EmptyRetriever())
    ]

    assert mcp_client.calls == [("search_web", {"query": "latest Betelgeuse news"})]
    assert events[-1]["type"] == "done"


@pytest.mark.asyncio
async def test_run_chat_can_scrape_page_after_web_search_identifies_target_url():
    history = [
        {"role": "system", "content": "You are AstroLlama."},
        {"role": "user", "content": "What current activities is RASC Winnipeg Centre advertising on their web site?"},
    ]
    settings = ChatSettings()
    mcp_client = FakeMCPClientWebFallback()

    def inspect_first_call(messages, tools):
        system_message = messages[0]["content"]
        assert "WEBSITE QUERY ROUTING:" in system_message
        assert "Do not switch to news or YouTube tools" in system_message

    def inspect_second_call(messages, tools):
        tool_messages = [msg for msg in messages if msg.get("role") == "tool"]
        assert tool_messages
        assert "call scrape_website once" in tool_messages[-1]["content"]

    llm_client = ScriptedLLMClient(
        [
            {
                "inspect": inspect_first_call,
                "tool_call": {"name": "search_web", "arguments": {"query": "RASC Winnipeg Centre website current activities"}},
            },
            {
                "inspect": inspect_second_call,
                "tool_call": {"name": "scrape_website", "arguments": {"url": "https://example.com/betelgeuse"}},
            },
            {"content": ["The site is advertising observing events and meetings."]},
        ]
    )

    events = [event async for event in run_chat(history, settings, llm_client, mcp_client, retriever=None)]

    assert mcp_client.calls == [
        (
            "search_web",
            {"query": "What current activities is RASC Winnipeg Centre advertising on their web site?"},
        ),
        ("scrape_website", {"url": "https://example.com/betelgeuse"}),
    ]
    assert events[-1]["type"] == "done"


@pytest.mark.asyncio
async def test_run_chat_blocks_news_detour_for_website_intent():
    history = [
        {"role": "system", "content": "You are AstroLlama."},
        {"role": "user", "content": "What current activities is RASC Winnipeg Centre advertising on their web site?"},
    ]
    settings = ChatSettings()
    mcp_client = FakeMCPClientWebFallback()

    def inspect_second_call(messages, tools):
        tool_messages = [msg for msg in messages if msg.get("role") == "tool"]
        assert tool_messages
        assert "is blocked for this query because the user asked for website content" in tool_messages[-1]["content"]

    llm_client = ScriptedLLMClient(
        [
            {"tool_call": {"name": "search_news", "arguments": {"keyword": "RASC Winnipeg Centre"}}},
            {
                "inspect": inspect_second_call,
                "tool_call": {
                    "name": "search_web",
                    "arguments": {"query": "RASC Winnipeg Centre"},
                },
            },
            {"content": ["Website content summarized from the center site."]},
        ]
    )

    events = [event async for event in run_chat(history, settings, llm_client, mcp_client, retriever=None)]

    assert mcp_client.calls == [
        (
            "search_web",
            {"query": "What current activities is RASC Winnipeg Centre advertising on their web site?"},
        )
    ]
    assert any(event["type"] == "tool_error" and event.get("name") == "search_news" for event in events)
    assert events[-1]["type"] == "done"


def test_prepare_llm_tool_content_formats_search_results_for_concise_summary():
    raw_result = json.dumps(
        [
            {
                "title": "RASC Winnipeg Centre",
                "url": "https://example.com/rasc",
                "description": "If you have access to the RASC Winnipeg Centre website, you might want to check the website directly.",
                "deep_results": [
                    "If you have access to the RASC Winnipeg Centre website, you might want to check the website directly.",
                    "Check their YouTube channel for more information.",
                ],
            },
            {
                "title": "RASC Winnipeg Centre",
                "url": "https://example.com/rasc",
                "description": "If you have access to the RASC Winnipeg Centre website, you might want to check the website directly.",
            },
        ]
    )

    prepared = _prepare_llm_tool_content("search_web", raw_result)

    assert "TOOL OUTPUT (web search results):" in prepared
    assert prepared.count(
        "If you have access to the RASC Winnipeg Centre website, you might want to check the website directly."
    ) == 1
    assert "Do not repeat the same sentence or recommendation more than once." in prepared