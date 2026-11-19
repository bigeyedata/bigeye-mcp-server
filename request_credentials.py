import os
from contextvars import ContextVar
from typing import FrozenSet, Optional, TypedDict
from urllib.parse import urlsplit


class RequestCredentials(TypedDict):
    api_key: str
    workspace_id: Optional[int]
    url: Optional[str]


class DisallowedUrlError(ValueError):
    pass


current_credentials: ContextVar[Optional[RequestCredentials]] = ContextVar(
    "bigeye_request_credentials", default=None
)


def allowed_hosts() -> FrozenSet[str]:
    raw = os.environ.get("BIGEYE_ALLOWED_HOSTS", "app.bigeye.com")
    return frozenset(h.strip().lower() for h in raw.split(",") if h.strip())


def normalize_url(raw: str) -> Optional[str]:
    """Turn a raw x-bigeye-url header into a base URL, or None if unset.

    Accepts either a full URL (https://app.bigeye.com) or a bare host
    (app.bigeye.com). Only https and hosts listed in BIGEYE_ALLOWED_HOSTS
    are accepted; anything else raises DisallowedUrlError.
    """
    raw = (raw or "").strip()
    if not raw:
        return None
    url = raw if "://" in raw else f"https://{raw}"
    try:
        parts = urlsplit(url)
        port = parts.port
    except ValueError as e:
        raise DisallowedUrlError(raw) from e
    host = (parts.hostname or "").lower()
    if (
        parts.scheme != "https"
        or parts.username
        or parts.password
        or port not in (None, 443)
        or parts.path not in ("", "/")
        or parts.query
        or parts.fragment
        or host not in allowed_hosts()
    ):
        raise DisallowedUrlError(raw)
    return f"https://{host}"
