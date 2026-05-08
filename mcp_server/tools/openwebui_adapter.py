from __future__ import annotations

import inspect
import json
import re
import sys
import types as pytypes
import uuid
from pathlib import Path
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


def install_openwebui_shims(downloads_dir: Path) -> None:
    """
    Install lightweight runtime shims for OpenWebUI-only imports.

    This allows unmodified OpenWebUI tools to import in AstroLlama.
    """
    if "open_webui" in sys.modules:
        return

    open_webui_mod = pytypes.ModuleType("open_webui")
    models_mod = pytypes.ModuleType("open_webui.models")
    users_mod = pytypes.ModuleType("open_webui.models.users")
    chats_mod = pytypes.ModuleType("open_webui.models.chats")
    files_model_mod = pytypes.ModuleType("open_webui.models.files")
    config_mod = pytypes.ModuleType("open_webui.config")
    routers_mod = pytypes.ModuleType("open_webui.routers")
    files_mod = pytypes.ModuleType("open_webui.routers.files")
    images_mod = pytypes.ModuleType("open_webui.routers.images")
    utils_mod = pytypes.ModuleType("open_webui.utils")
    misc_mod = pytypes.ModuleType("open_webui.utils.misc")

    class _ShimUser:
        def __init__(self, user_id: str | int = "anonymous") -> None:
            self.id = user_id

    class UserModel:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class Users:
        @staticmethod
        async def get_user_by_id(user_id: str | int):
            return _ShimUser(user_id)

    class _UploadedFileItem:
        def __init__(self, file_id: str) -> None:
            self.id = file_id
            self.filename = file_id
            self.path = str(downloads_dir / file_id)

    class Chats:
        @staticmethod
        async def add_message_files_by_id_and_message_id(chat_id, message_id, files):
            return True

        @staticmethod
        async def get_chat_by_id(chat_id):
            return {"id": chat_id, "chat": {"history": {"messages": {}}}}

        @staticmethod
        async def update_chat_by_id(chat_id, chat_data):
            return True

    class Files:
        @staticmethod
        async def get_file_by_id(file_id):
            target = downloads_dir / str(file_id)
            if target.exists():
                return {
                    "id": str(file_id),
                    "filename": target.name,
                    "path": str(target),
                }
            return None

    async def upload_file_handler(request=None, file=None, metadata=None, process=False, user=None):
        # Persist uploads to AstroLlama downloads storage with a stable id.
        file_name = getattr(file, "filename", None) or f"owui_{uuid.uuid4().hex}.bin"
        safe_name = re.sub(r"[^\w.\-]", "_", file_name)
        file_id = f"owui_{uuid.uuid4().hex}_{safe_name}"
        target = downloads_dir / file_id

        content = b""
        fobj = getattr(file, "file", None)
        if fobj is not None and hasattr(fobj, "read"):
            maybe = fobj.read()
            content = maybe if isinstance(maybe, (bytes, bytearray)) else str(maybe).encode("utf-8", errors="ignore")

        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        return _UploadedFileItem(file_id=file_id)

    class GenerateImageForm:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

    async def image_generations(request=None, form_data=None, user=None):
        # Placeholder shim: OpenWebUI-native image generation routing is unavailable.
        return []

    async def get_image_data(data):
        if isinstance(data, bytes):
            return data, "image/png"
        if isinstance(data, str) and data.startswith("data:") and "," in data:
            encoded = data.split(",", 1)[1]
            try:
                import base64
                return base64.b64decode(encoded), "image/png"
            except Exception:
                return b"", "application/octet-stream"
        return b"", "application/octet-stream"

    async def upload_image(request=None, image_data=None, user=None, file_name=None, content_type="image/png"):
        name = file_name or f"owui_img_{uuid.uuid4().hex}.png"
        safe_name = re.sub(r"[^\w.\-]", "_", name)
        file_id = f"owui_{uuid.uuid4().hex}_{safe_name}"
        target = downloads_dir / file_id
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(image_data or b"")
        return _UploadedFileItem(file_id=file_id), f"/api/files/{file_id}"

    def get_last_user_message_item(messages):
        if not messages:
            return None
        for item in reversed(messages):
            if isinstance(item, dict) and item.get("role") == "user":
                return item
        return messages[-1]

    users_mod.Users = Users
    users_mod.UserModel = UserModel
    chats_mod.Chats = Chats
    files_model_mod.Files = Files
    files_mod.upload_file_handler = upload_file_handler
    images_mod.GenerateImageForm = GenerateImageForm
    images_mod.image_generations = image_generations
    images_mod.get_image_data = get_image_data
    images_mod.upload_image = upload_image
    misc_mod.get_last_user_message_item = get_last_user_message_item
    config_mod.UPLOAD_DIR = downloads_dir

    sys.modules["open_webui"] = open_webui_mod
    sys.modules["open_webui.models"] = models_mod
    sys.modules["open_webui.models.users"] = users_mod
    sys.modules["open_webui.models.chats"] = chats_mod
    sys.modules["open_webui.models.files"] = files_model_mod
    sys.modules["open_webui.config"] = config_mod
    sys.modules["open_webui.routers"] = routers_mod
    sys.modules["open_webui.routers.files"] = files_mod
    sys.modules["open_webui.routers.images"] = images_mod
    sys.modules["open_webui.utils"] = utils_mod
    sys.modules["open_webui.utils.misc"] = misc_mod


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
    kwargs = dict(arguments or {})
    sig = inspect.signature(spec.method)

    async def _noop_event_emitter(_event: Any) -> None:
        return None

    # Inject common OpenWebUI runtime placeholders when requested.
    if "__event_emitter__" in sig.parameters and "__event_emitter__" not in kwargs:
        kwargs["__event_emitter__"] = _noop_event_emitter
    if "__user__" in sig.parameters and "__user__" not in kwargs:
        kwargs["__user__"] = {}
    if "__request__" in sig.parameters and "__request__" not in kwargs:
        kwargs["__request__"] = None
    if "__messages__" in sig.parameters and "__messages__" not in kwargs:
        kwargs["__messages__"] = []
    if "__files__" in sig.parameters and "__files__" not in kwargs:
        kwargs["__files__"] = []
    if "__model__" in sig.parameters and "__model__" not in kwargs:
        kwargs["__model__"] = ""
    if "__task__" in sig.parameters and "__task__" not in kwargs:
        kwargs["__task__"] = ""
    if "__metadata__" in sig.parameters and "__metadata__" not in kwargs:
        kwargs["__metadata__"] = {}
    if "__tools__" in sig.parameters and "__tools__" not in kwargs:
        kwargs["__tools__"] = {}

    # Generic fallback for other OpenWebUI magic args.
    for pname in sig.parameters:
        if not pname.startswith("__") or pname in kwargs:
            continue
        if pname.endswith("_id__") or pname in {"__chat_id__", "__message_id__", "__session_id__"}:
            kwargs[pname] = ""
        else:
            kwargs[pname] = {}

    result = spec.method(**kwargs)
    if inspect.isawaitable(result):
        result = await result

    # OpenWebUI tools may return (payload, message); prefer the textual message.
    if isinstance(result, tuple) and len(result) >= 2 and isinstance(result[1], str):
        return result[1]

    # FastAPI responses or similar objects with body/content.
    body = getattr(result, "body", None)
    if isinstance(body, (bytes, bytearray)):
        try:
            return body.decode("utf-8", errors="replace")
        except Exception:
            return str(result)

    if isinstance(result, str):
        return result
    if isinstance(result, (dict, list)):
        return json.dumps(result, ensure_ascii=False)
    return str(result)
