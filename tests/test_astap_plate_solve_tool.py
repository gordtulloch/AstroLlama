from __future__ import annotations

import json
import sys

import pytest

sys.path.insert(0, r"c:\Projects\AstroLlama")

from mcp_server.tools.astap_plate_solve_tool import Tools


def test_astap_binary_resolution_uses_explicit_valve_path(tmp_path):
    tool = Tools()
    binary = tmp_path / "astap_cli"
    binary.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    binary.chmod(0o755)

    tool.valves.astap_binary_path = str(binary)

    assert tool._resolve_astap_binary() == str(binary)


@pytest.mark.asyncio
async def test_astap_status_reports_unavailable_when_no_binary(monkeypatch):
    tool = Tools()
    monkeypatch.setattr(tool, "_resolve_astap_binary", lambda: None)

    payload = json.loads(await tool.astap_status())

    assert payload["available"] is False
    assert "binary_candidates" in payload
    assert payload["binary"] == tool.valves.astap_binary_path


@pytest.mark.asyncio
async def test_astap_plate_solve_requires_filename():
    tool = Tools()

    with pytest.raises(ValueError, match="fits_filename is required"):
        await tool.astap_plate_solve("")