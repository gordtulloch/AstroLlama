from __future__ import annotations

import inspect
import re
from dataclasses import dataclass
from typing import Any, Callable, get_args, get_origin, Literal, Union

import mcp.types as types


@dataclass
class OpenWebUIToolSpec:
    """Internal spec for an OpenWebUI-style tool method."""

    name: str
    description: str
    input_schema: dict[str, Any]
    method: Callable[..., Any]


def _parse_sphinx_docstring(doc: str) -> tuple[str, dict[str, str], str]:
    """
    Parse a Sphinx-style docstring.

    Returns a tuple of (summary, param_descriptions, returns_description).
    """
    text = (doc or "").strip()
    if not text:
        return "", {}, ""

    lines = [ln.rstrip() for ln in text.splitlines()]
    summary_lines: list[str] = []
    param_desc: dict[str, str] = {}
    returns_desc = ""

    in_summary = True
    for line in lines:
        stripped = line.strip()
        if stripped.startswith(":param "):
            in_summary = False
            m = re.match(r":param\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*:\s*(.*)", stripped)
            if m:
                param_desc[m.group(1)] = m.group(2).strip()
            continue
        if stripped.startswith(":returns:") or stripped.startswith(":return:"):
            in_summary = False
            parts = stripped.split(":", 2)
            if len(parts) == 3:
                returns_desc = parts[2].strip()
            continue
        if stripped.startswith(":rtype:") or stripped.startswith(":type "):
            in_summary = False
            continue
        if in_summary and stripped:
            summary_lines.append(stripped)

    summary = " ".join(summary_lines).strip()
    return summary, param_desc, returns_desc


def _annotation_to_schema(annotation: Any) -> dict[str, Any]:
    """Map Python type annotations to JSON Schema fragments."""
    if annotation is inspect._empty:
        return {"type": "string"}

    origin = get_origin(annotation)
    args = get_args(annotation)

    if origin is Literal:
        values = list(args)
        if not values:
            return {"type": "string"}
        if all(isinstance(v, str) for v in values):
            return {"type": "string", "enum": values}
        if all(isinstance(v, int) for v in values):
            return {"type": "integer", "enum": values}
        if all(isinstance(v, (int, float)) for v in values):
            return {"type": "number", "enum": values}
        return {"type": "string", "enum": [str(v) for v in values]}

    # Optional[T] is Union[T, None]
    if origin is Union:
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return _annotation_to_schema(non_none[0])
        return {"type": "string"}

    if origin in (list, tuple):
        item_schema = _annotation_to_schema(args[0]) if args else {"type": "string"}
        return {"type": "array", "items": item_schema}

    if origin is dict:
        return {"type": "object"}

    if annotation is str:
        return {"type": "string"}
    if annotation is int:
        return {"type": "integer"}
    if annotation is float:
        return {"type": "number"}
    if annotation is bool:
        return {"type": "boolean"}

    return {"type": "string"}


def build_tool_specs(tools_obj: Any) -> dict[str, OpenWebUIToolSpec]:
    """Build OpenWebUIToolSpec entries from a Tools object instance."""
    specs: dict[str, OpenWebUIToolSpec] = {}

    for name, method in inspect.getmembers(tools_obj, predicate=inspect.ismethod):
        if name.startswith("_"):
            continue

        sig = inspect.signature(method)
        doc = inspect.getdoc(method) or ""
        summary, param_desc, returns_desc = _parse_sphinx_docstring(doc)

        properties: dict[str, Any] = {}
        required: list[str] = []

        for param_name, param in sig.parameters.items():
            if param_name == "self":
                continue
            schema = _annotation_to_schema(param.annotation)
            if param_name in param_desc and param_desc[param_name]:
                schema["description"] = param_desc[param_name]
            if param.default is not inspect._empty:
                schema["default"] = param.default
            else:
                required.append(param_name)
            properties[param_name] = schema

        input_schema: dict[str, Any] = {
            "type": "object",
            "properties": properties,
            "required": required,
        }

        description = summary or f"OpenWebUI-compatible tool method: {name}."
        if returns_desc:
            description = f"{description} Returns: {returns_desc}"

        specs[name] = OpenWebUIToolSpec(
            name=name,
            description=description,
            input_schema=input_schema,
            method=method,
        )

    return specs


def specs_to_mcp_tools(specs: dict[str, OpenWebUIToolSpec]) -> list[types.Tool]:
    """Convert OpenWebUIToolSpec map to MCP tool descriptors."""
    out: list[types.Tool] = []
    for spec in specs.values():
        out.append(
            types.Tool(
                name=spec.name,
                description=spec.description,
                inputSchema=spec.input_schema,
            )
        )
    return out


async def invoke_spec(spec: OpenWebUIToolSpec, arguments: dict[str, Any]) -> str:
    """Invoke a tool spec method and coerce output to string for MCP."""
    result = spec.method(**arguments)
    if inspect.isawaitable(result):
        result = await result
    if isinstance(result, str):
        return result
    return str(result)
