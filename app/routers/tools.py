from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from app.services.auth import require_auth
from common.valves_store import ValvesStore

router = APIRouter()
_store = ValvesStore()


class ToolValvesUpdateRequest(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)


def _validate_value(value: Any, schema: dict[str, Any], field_name: str) -> str | None:
    expected_type = schema.get("type")
    enum_values = schema.get("enum")

    if enum_values is not None and value not in enum_values:
        return f"{field_name}: value must be one of {enum_values}"

    if expected_type == "string":
        if not isinstance(value, str):
            return f"{field_name}: value must be a string"
        min_len = schema.get("minLength")
        max_len = schema.get("maxLength")
        if isinstance(min_len, int) and len(value) < min_len:
            return f"{field_name}: must be at least {min_len} characters"
        if isinstance(max_len, int) and len(value) > max_len:
            return f"{field_name}: must be at most {max_len} characters"
        return None

    if expected_type == "boolean":
        if not isinstance(value, bool):
            return f"{field_name}: value must be true or false"
        return None

    if expected_type == "integer":
        if isinstance(value, bool) or not isinstance(value, int):
            return f"{field_name}: value must be an integer"
        minimum = schema.get("minimum")
        maximum = schema.get("maximum")
        if isinstance(minimum, (int, float)) and value < minimum:
            return f"{field_name}: value must be >= {minimum}"
        if isinstance(maximum, (int, float)) and value > maximum:
            return f"{field_name}: value must be <= {maximum}"
        return None

    if expected_type == "number":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return f"{field_name}: value must be a number"
        numeric_value = float(value)
        minimum = schema.get("minimum")
        maximum = schema.get("maximum")
        if isinstance(minimum, (int, float)) and numeric_value < float(minimum):
            return f"{field_name}: value must be >= {minimum}"
        if isinstance(maximum, (int, float)) and numeric_value > float(maximum):
            return f"{field_name}: value must be <= {maximum}"
        return None

    return None


def _validate_payload(values: dict[str, Any], schema: dict[str, Any]) -> list[str]:
    props = schema.get("properties") or {}
    if not isinstance(props, dict):
        return ["Invalid schema: properties is missing or malformed"]

    errors: list[str] = []

    for key in values:
        if key not in props:
            errors.append(f"{key}: unknown valve field")

    for key, value in values.items():
        field_schema = props.get(key)
        if not isinstance(field_schema, dict):
            continue
        err = _validate_value(value, field_schema, key)
        if err:
            errors.append(err)

    return errors


async def _fetch_tool_valves_snapshot(request: Request) -> dict[str, Any]:
    mcp_client = request.app.state.mcp_client
    if not mcp_client.available:
        raise HTTPException(status_code=503, detail="MCP server is unavailable")

    try:
        raw = await mcp_client.read_resource("astro://info/tool_valves")
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("Tool valves payload is not an object")
        return data
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to read tool valves: {exc}") from exc


@router.get("/tools/valves")
async def get_tool_valves(
    request: Request,
    _claims: dict[str, Any] | None = Depends(require_auth),
) -> dict[str, Any]:
    data = await _fetch_tool_valves_snapshot(request)
    data["persisted_overrides"] = _store.list_all()
    return data


@router.put("/tools/{tool_name}/valves")
async def update_tool_valves(
    tool_name: str,
    body: ToolValvesUpdateRequest,
    request: Request,
    _claims: dict[str, Any] | None = Depends(require_auth),
) -> dict[str, Any]:
    data = await _fetch_tool_valves_snapshot(request)
    valves = data.get("valves") or {}
    if tool_name not in valves:
        raise HTTPException(status_code=404, detail=f"Unknown tool module '{tool_name}'")

    tool_info = valves[tool_name]
    schema = tool_info.get("schema") or {}
    current_values = tool_info.get("values") or {}

    if not isinstance(current_values, dict):
        raise HTTPException(status_code=500, detail="Malformed valve values from MCP")

    merged = dict(current_values)
    merged.update(body.values)

    validation_errors = _validate_payload(merged, schema)
    if validation_errors:
        raise HTTPException(status_code=422, detail={"errors": validation_errors})

    _store.set(tool_name, merged)

    # Trigger MCP refresh path and return updated view.
    refreshed = await _fetch_tool_valves_snapshot(request)
    refreshed_tool = (refreshed.get("valves") or {}).get(tool_name, {})

    return {
        "tool_name": tool_name,
        "values": refreshed_tool.get("values", merged),
        "schema": refreshed_tool.get("schema", schema),
    }
