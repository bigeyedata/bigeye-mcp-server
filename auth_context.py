"""Per-request Bigeye auth context, carried as a contextvar.

In stdio mode the context is unset and BigeyeAPIClient / config fall back
to env vars (BIGEYE_API_KEY, BIGEYE_BASE_URL, BIGEYE_WORKSPACE_ID). In
HTTP mode the bearer middleware sets one of these per request, so each
incoming MCP call uses the caller's own Bigeye credentials.
"""

from __future__ import annotations

import contextvars
from dataclasses import dataclass


@dataclass(frozen=True)
class BigeyeAuthContext:
    """Resolved Bigeye credentials for a single request.

    api_url is the Bigeye instance base (e.g. https://app.bigeye.com).
    api_key is the user's Bigeye API key, used as ``apikey <key>`` in the
    Authorization header on upstream calls. workspace_id must be int.
    """

    api_url: str
    api_key: str
    workspace_id: int


_ctx: contextvars.ContextVar[BigeyeAuthContext | None] = contextvars.ContextVar(
    "bigeye_auth_ctx", default=None
)


def set_auth_context(ctx: BigeyeAuthContext) -> contextvars.Token:
    return _ctx.set(ctx)


def reset_auth_context(token: contextvars.Token) -> None:
    _ctx.reset(token)


def current_auth_context() -> BigeyeAuthContext | None:
    return _ctx.get()
