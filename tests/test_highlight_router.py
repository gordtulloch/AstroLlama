from app.routers.highlight import _render


def test_render_fortran_fence_is_highlighted():
    src = """```fortran
program hello
  print *, \"hi\"
end program hello
```"""

    html = _render(src)

    assert 'class="code-block"' in html
    assert '<div class="code-lang">fortran</div>' in html
    assert "program" in html


def test_render_crlf_fence_is_parsed():
    src = "```fortran\r\nprogram hello\r\nend program hello\r\n```"

    html = _render(src)

    assert 'class="code-block"' in html
    assert '<div class="code-lang">fortran</div>' in html


def test_render_fortran_alias_f90_is_resolved():
    src = """```f90
program x
end program x
```"""

    html = _render(src)

    assert '<div class="code-lang">fortran</div>' in html
    assert 'class="code-block"' in html
