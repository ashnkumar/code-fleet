"""Request dispatcher.

Deliberately framework-free: ``handle`` takes a method, a path and an optional
body, and returns ``(status, payload)``. That keeps the whole surface testable
with nothing but pytest.
"""

from linkstash.store import LinkStore

_store = LinkStore()

Response = tuple[int, dict]


def handle(method: str, path: str, body: dict | None = None) -> Response:
    """Route a request to a handler."""
    if method == "POST" and path == "/links":
        return create_link(body or {})
    if method == "GET" and path.startswith("/links/"):
        return read_link(path.removeprefix("/links/"))
    return 404, {"error": "not found"}


def create_link(body: dict) -> Response:
    url = body.get("url")
    if not url:
        return 400, {"error": "url is required"}
    try:
        code = _store.save(url)
    except ValueError as exc:
        return 400, {"error": str(exc)}
    return 201, {"code": code, "short_url": _store.short_url(code)}


def read_link(code: str) -> Response:
    url = _store.resolve(code)
    if url is None:
        return 404, {"error": "unknown code"}
    return 200, {"code": code, "url": url}
