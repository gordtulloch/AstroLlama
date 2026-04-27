"""POST /api/highlight — server-side syntax highlighting via Pygments.
GET  /api/highlight/styles — returns the CSS needed by the frontend.
"""
from __future__ import annotations

import html as html_module
import re

import markdown as markdown_lib
from fastapi import APIRouter
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import TextLexer, get_lexer_by_name
from pygments.util import ClassNotFound

router = APIRouter()

_STYLE = "monokai"
_FORMATTER = HtmlFormatter(style=_STYLE, cssclass="highlight")

# Matches fenced code blocks: ```lang\n code \n```
# \n? before closing fence tolerates a trailing newline inside the block.
_CODE_BLOCK_RE = re.compile(r"```(\w*)[^\S\r\n]*\n([\s\S]*?\n?)```", re.MULTILINE)

# Matches bare "Image: /api/files/<name>.ext" lines produced by generate_map.
_IMAGE_LINE_RE = re.compile(
    r"(?m)^Image:\s*(/api/files/[^\s]+\.(?:png|jpg|jpeg|gif|webp))$",
    re.IGNORECASE,
)

# Matches markdown images whose URL points to /api/files/, e.g. ![alt](/api/files/foo.png)
_MD_IMAGE_RE = re.compile(
    r"!\[[^\]]*\]\((/api/files/[^\s)]+\.(?:png|jpg|jpeg|gif|webp))\)",
    re.IGNORECASE,
)

_IMG_EXTS = re.compile(r"\.(?:png|jpg|jpeg|gif|webp)$", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _make_thumbnail_html(url_raw: str) -> str:
    url = html_module.escape(url_raw, quote=True)
    return (
        f'\n<div class="map-thumbnail-wrap">'
        f'<a href="{url}" target="_blank" rel="noopener noreferrer">'
        f'<img src="{url}" class="map-thumbnail" alt="Star map" />'
        f'</a></div>\n'
    )


def _render_prose(text: str) -> str:
    """Render plain prose with full markdown support (no fenced code — handled separately)."""
    # Convert bare "Image: /api/files/..." lines into thumbnail HTML.
    text = _IMAGE_LINE_RE.sub(lambda m: _make_thumbnail_html(m.group(1)), text)
    # Convert markdown images pointing at /api/files/ into thumbnail HTML
    # BEFORE markdown() turns them into unconstrained <img> tags.
    text = _MD_IMAGE_RE.sub(lambda m: _make_thumbnail_html(m.group(1)), text)
    return markdown_lib.markdown(
        text.strip(),
        extensions=["tables", "sane_lists"],
    )


def _render(text: str) -> str:
    """Parse *text* for fenced code blocks and return a fully highlighted HTML string."""
    result: list[str] = []
    last_end = 0

    for m in _CODE_BLOCK_RE.finditer(text):
        before = text[last_end : m.start()]
        if before.strip():
            result.append(_render_prose(before))

        lang = m.group(1).strip().lower()
        code = m.group(2)

        if lang == "html":
            # Render raw HTML directly (existing behaviour)
            result.append(f'<div class="rendered-html">{code}</div>')
        else:
            try:
                lexer = get_lexer_by_name(lang, stripall=True) if lang else TextLexer()
            except ClassNotFound:
                lexer = TextLexer()

            label = html_module.escape(lang) if lang else "text"
            highlighted = highlight(code, lexer, _FORMATTER)
            result.append(
                f'<div class="code-block">'
                f'<div class="code-lang">{label}</div>'
                f"{highlighted}"
                f"</div>"
            )

        last_end = m.end()

    remaining = text[last_end:]
    if remaining.strip():
        result.append(_render_prose(remaining))

    if not result:
        return _render_prose(text)
    return "\n".join(result)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

class HighlightRequest(BaseModel):
    text: str


class HighlightResponse(BaseModel):
    html: str


@router.get("/api/highlight/styles", response_class=PlainTextResponse)
async def get_styles() -> str:
    """Return the Pygments CSS that the frontend injects into <head>."""
    return _FORMATTER.get_style_defs(".highlight")


@router.post("/api/highlight", response_model=HighlightResponse)
async def highlight_text(req: HighlightRequest) -> HighlightResponse:
    return HighlightResponse(html=_render(req.text))
