from contextvars import ContextVar
from typing import Optional, TypedDict


class RequestCredentials(TypedDict):
    api_key: str
    workspace_id: Optional[int]


current_credentials: ContextVar[Optional[RequestCredentials]] = ContextVar(
    "bigeye_request_credentials", default=None
)
