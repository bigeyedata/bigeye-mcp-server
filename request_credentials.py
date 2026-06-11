from contextvars import ContextVar
from typing import Optional, TypedDict


class RequestCredentials(TypedDict):
    api_key: str
    workspace_id: Optional[int]
    instance: Optional[str]


current_credentials: ContextVar[Optional[RequestCredentials]] = ContextVar(
    "bigeye_request_credentials", default=None
)


def normalize_instance(raw: str) -> Optional[str]:
    """Turn a raw x-bigeye-instance header into a base URL, or None if unset.

    Accepts either a full URL (https://staging.bigeye.com) or a bare host
    (staging.bigeye.com), defaulting to https and trimming a trailing slash.
    """
    raw = (raw or "").strip()
    if not raw:
        return None
    url = raw if raw.startswith(("http://", "https://")) else f"https://{raw}"
    return url.rstrip("/")
