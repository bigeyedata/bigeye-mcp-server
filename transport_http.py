"""Streamable-HTTP transport for the Bigeye MCP server (BYOK).

Builds a Starlette ASGI app that:

- mounts ``POST /mcp`` on top of
  :class:`mcp.server.streamable_http_manager.StreamableHTTPSessionManager`,
  which handles the JSON-RPC over HTTP + SSE protocol;
- exposes ``GET /health`` for container health checks (no auth);
- gates every other path with :class:`BigeyeBearerAuthMiddleware`, which
  translates ``Authorization: Bearer <bigeye-api-key>`` plus the two
  ``X-Bigeye-*`` request headers into a per-request auth contextvar.

There is no Authorization Server in this transport: the bearer is the
caller's Bigeye API key, used verbatim on every upstream call. Validation
happens implicitly — Bigeye returns 401/403 if the key is wrong, and
that response is propagated back to the MCP client.
"""

from __future__ import annotations

import contextlib
import logging
from typing import AsyncIterator

from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route
from starlette.types import ASGIApp, Receive, Scope, Send

from auth_context import (
    BigeyeAuthContext,
    reset_auth_context,
    set_auth_context,
)

logger = logging.getLogger(__name__)

# Custom headers the MCP client must send alongside Authorization.
HEADER_BASE_URL = "x-bigeye-base-url"
HEADER_WORKSPACE_ID = "x-bigeye-workspace-id"

# Paths that bypass the bearer check.
_UNAUTHENTICATED_PATHS = ("/health",)


def _unauthorized(reason: str, status_code: int = 401) -> JSONResponse:
    return JSONResponse(
        {"error": "unauthorized", "reason": reason},
        status_code=status_code,
        headers={"WWW-Authenticate": f'Bearer error="{reason}"'},
    )


def _bad_request(reason: str) -> JSONResponse:
    return JSONResponse({"error": "bad_request", "reason": reason}, status_code=400)


class BigeyeBearerAuthMiddleware:
    """ASGI middleware: pull credentials off the request, install them in
    the auth contextvar for the duration of the call.

    Required headers:

    - ``Authorization: Bearer <bigeye-api-key>``
    - ``X-Bigeye-Base-Url: https://app.bigeye.com``
    - ``X-Bigeye-Workspace-Id: 12345``  (must parse as int)
    """

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path: str = scope.get("path") or ""
        if any(path.startswith(p) for p in _UNAUTHENTICATED_PATHS):
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive=receive)
        header = request.headers.get("authorization", "")
        if not header.lower().startswith("bearer "):
            await _unauthorized("missing_token")(scope, receive, send)
            return
        api_key = header.split(" ", 1)[1].strip()
        if not api_key:
            await _unauthorized("missing_token")(scope, receive, send)
            return

        base_url = request.headers.get(HEADER_BASE_URL, "").strip()
        if not base_url:
            await _bad_request(f"missing required header: {HEADER_BASE_URL}")(scope, receive, send)
            return

        workspace_id_raw = request.headers.get(HEADER_WORKSPACE_ID, "").strip()
        if not workspace_id_raw:
            await _bad_request(f"missing required header: {HEADER_WORKSPACE_ID}")(scope, receive, send)
            return
        try:
            workspace_id = int(workspace_id_raw)
        except ValueError:
            await _bad_request(f"{HEADER_WORKSPACE_ID} must be an integer")(scope, receive, send)
            return

        ctx = BigeyeAuthContext(
            api_url=base_url.rstrip("/"),
            api_key=api_key,
            workspace_id=workspace_id,
        )
        token = set_auth_context(ctx)
        try:
            await self.app(scope, receive, send)
        finally:
            reset_auth_context(token)


class _NormalizeMcpPath:
    """Rewrite bare ``/mcp`` → ``/mcp/`` in the ASGI scope so the Mount's
    path match succeeds without a client-breaking 307 redirect.

    HTTP clients strip the Authorization header on redirects to avoid
    leaking credentials across origins. A redirect from /mcp → /mcp/ on
    the same origin therefore causes the follow-up request to fail auth.
    Rewriting in-place avoids the redirect entirely. (Same trick the
    bigeye-context-guardian MCP transport uses.)"""

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http" and scope.get("path") == "/mcp":
            scope = dict(scope)
            scope["path"] = "/mcp/"
            scope["raw_path"] = b"/mcp/"
        await self.app(scope, receive, send)


async def health(_request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok"})


def build_app() -> Starlette:
    """Wire FastMCP's underlying server into a Starlette app with bearer
    auth + path normalization. Imports ``server`` lazily so a missing
    BIGEYE_API_KEY env var doesn't trip stdio-mode validation: HTTP mode
    populates credentials from request headers, not env."""
    # Lazy import to avoid stdio-mode env var pre-flight noise. The
    # underlying FastMCP instance carries every @mcp.tool() registered
    # via decorator side-effects in server.py.
    from server import mcp  # noqa: WPS433

    session_manager = StreamableHTTPSessionManager(
        app=mcp._mcp_server,
        json_response=False,
        stateless=False,
    )

    @contextlib.asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        async with session_manager.run():
            yield

    async def mcp_asgi(scope: Scope, receive: Receive, send: Send) -> None:
        await session_manager.handle_request(scope, receive, send)

    app = Starlette(
        debug=False,
        routes=[
            Route("/health", health, methods=["GET"]),
            Mount("/mcp", app=mcp_asgi),
        ],
        middleware=[
            Middleware(_NormalizeMcpPath),
            Middleware(BigeyeBearerAuthMiddleware),
        ],
        lifespan=lifespan,
    )
    app.router.redirect_slashes = False
    return app


def run_http(host: str = "0.0.0.0", port: int = 9101) -> None:
    import uvicorn

    logging.basicConfig(level=logging.INFO)
    uvicorn.run(build_app(), host=host, port=port)
