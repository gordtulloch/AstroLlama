"""Regression tests for MCP tool loading and invocation behavior.

These tests protect the shared MCP tool integration layer so that adding or
changing one tool does not break unrelated tools.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, r"c:\Projects\AstroLlama")

import mcp_server.server as mcp_server
from mcp_server.tools.get_weather_tool import Tools as WeatherTools
from mcp_server.tools.openwebui_adapter import OpenWebUIToolSpec, invoke_spec


def test_build_registry_skips_broken_tools_and_ignores_duplicates(monkeypatch):
    class GoodTools:
        async def shared_tool(self, query: str) -> str:
            """Primary shared tool.

            :param query: Search text.
            :returns: Result text.
            """
            return f"ok: {query}"

    class BrokenTools:
        def __init__(self) -> None:
            raise RuntimeError("broken module")

    class DuplicateTools:
        async def shared_tool(self, query: str) -> str:
            """Duplicate tool that should be ignored.

            :param query: Duplicate input.
            :returns: Duplicate output.
            """
            return f"dup: {query}"

    class GoodModule:
        Tools = GoodTools

    class BrokenModule:
        Tools = BrokenTools

    class DuplicateModule:
        Tools = DuplicateTools

    tool_files = [
        Path("good.py"),
        Path("broken.py"),
        Path("duplicate.py"),
    ]
    modules = {
        "good": GoodModule,
        "broken": BrokenModule,
        "duplicate": DuplicateModule,
    }

    monkeypatch.setattr(mcp_server, "_iter_root_tool_files", lambda: tool_files)
    monkeypatch.setattr(mcp_server, "_load_module_from_file", lambda py_file: modules[py_file.stem])

    specs, mcp_tools, tools_objects = mcp_server._build_openwebui_tool_registry()

    assert "shared_tool" in specs
    assert len(specs) == 1
    assert [tool.name for tool in mcp_tools] == ["shared_tool"]
    assert "good" in tools_objects
    assert "broken" not in tools_objects


@pytest.mark.asyncio
async def test_invoke_spec_injects_openwebui_runtime_kwargs():
    observed = {}

    async def method(
        query: str,
        __event_emitter__=None,
        __user__=None,
        __request__=None,
        __messages__=None,
        __files__=None,
        __model__=None,
        __task__=None,
        __metadata__=None,
        __tools__=None,
        __chat_id__=None,
    ):
        observed.update(
            {
                "query": query,
                "event_emitter_is_callable": callable(__event_emitter__),
                "user": __user__,
                "request": __request__,
                "messages": __messages__,
                "files": __files__,
                "model": __model__,
                "task": __task__,
                "metadata": __metadata__,
                "tools": __tools__,
                "chat_id": __chat_id__,
            }
        )
        return {"ok": True}

    spec = OpenWebUIToolSpec(
        name="dummy",
        description="dummy",
        input_schema={"type": "object", "properties": {}},
        method=method,
    )

    result = await invoke_spec(spec, {"query": "hello"})

    assert json.loads(result) == {"ok": True}
    assert observed["query"] == "hello"
    assert observed["event_emitter_is_callable"] is True
    assert observed["user"] == {}
    assert observed["request"] is None
    assert observed["messages"] == []
    assert observed["files"] == []
    assert observed["model"] == ""
    assert observed["task"] == ""
    assert observed["metadata"] == {}
    assert observed["tools"] == {}
    assert observed["chat_id"] == ""


@pytest.mark.asyncio
async def test_call_tool_failure_does_not_break_other_tools(monkeypatch):
    async def ok_method(query: str) -> str:
        return f"ok:{query}"

    async def fail_method(query: str) -> str:
        raise RuntimeError("intentional failure")

    specs = {
        "ok_tool": OpenWebUIToolSpec(
            name="ok_tool",
            description="ok",
            input_schema={"type": "object", "properties": {"query": {"type": "string"}}},
            method=ok_method,
        ),
        "fail_tool": OpenWebUIToolSpec(
            name="fail_tool",
            description="fail",
            input_schema={"type": "object", "properties": {"query": {"type": "string"}}},
            method=fail_method,
        ),
    }

    monkeypatch.setattr(mcp_server, "_get_openwebui_tool_registry", lambda: (specs, []))

    failed = await mcp_server.call_tool("fail_tool", {"query": "x"})
    succeeded = await mcp_server.call_tool("ok_tool", {"query": "x"})

    assert "Error executing fail_tool" in failed[0].text
    assert "intentional failure" in failed[0].text
    assert succeeded[0].text == "ok:x"


@pytest.mark.asyncio
async def test_get_weather_accepts_location_and_geocodes(monkeypatch):
    observed = {}

    async def fake_geocode(location: str, count: int = 1):
        observed["geocode"] = {"location": location, "count": count}
        return (
            [
                {
                    "name": "Edinburgh",
                    "admin1": "Scotland",
                    "country": "United Kingdom",
                    "latitude": 55.9533,
                    "longitude": -3.1883,
                }
            ],
            location,
        )

    async def fake_weather(
        latitude: float,
        longitude: float,
        location_name: str | None = None,
        temperature_unit: str = "celsius",
        wind_speed_unit: str = "kmh",
    ) -> str:
        observed["weather"] = {
            "latitude": latitude,
            "longitude": longitude,
            "location_name": location_name,
            "temperature_unit": temperature_unit,
            "wind_speed_unit": wind_speed_unit,
        }
        return "Current Weather: Edinburgh, Scotland, United Kingdom"

    monkeypatch.setattr(
        "mcp_server.data_sources.open_meteo.fetch_open_meteo_geocode_results",
        fake_geocode,
    )
    monkeypatch.setattr(
        "mcp_server.data_sources.open_meteo.fetch_open_meteo_weather",
        fake_weather,
    )

    tool = WeatherTools()
    result = await tool.get_weather(location="Edinburgh Scotland")

    assert result.startswith("Current Weather: Edinburgh")
    assert observed["geocode"] == {"location": "Edinburgh Scotland", "count": 1}
    assert observed["weather"]["latitude"] == 55.9533
    assert observed["weather"]["longitude"] == -3.1883
    assert observed["weather"]["location_name"] == "Edinburgh, Scotland, United Kingdom"


def test_iter_root_tool_files_filters_non_tool_files():
    names = [path.name for path in mcp_server._iter_root_tool_files()]

    assert "__init__.py" not in names
    assert "openwebui_adapter.py" not in names
    assert all(name.endswith(".py") for name in names)
    assert names, "Expected at least one MCP tool file"
