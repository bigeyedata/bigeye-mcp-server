"""
Telemetry for the Bigeye MCP Server.

Reports anonymous, metadata-only usage analytics (which tool was called, how long
it took, success/error) to Bigeye's Datadog org so we can understand how the MCP
server is used in the field.

Design notes:
- Opt-out. Controlled by the BIGEYE_TELEMETRY env var (default ON). See config.py.
- No Datadog Agent required. Events are POSTed directly to Datadog's browser-logs
  intake endpoint, authenticating with a public Datadog *client token* (safe to
  commit / distribute -- it can only submit logs, never read or manage the org).
- Never blocks or breaks a tool call. Events are enqueued (non-blocking, dropped
  if the queue is full) and shipped from a background daemon thread. Every network
  operation is wrapped in a catch-all; telemetry failures are silent.
- Metadata only. We capture the tool name, duration, status, and error *type* --
  never tool arguments or response bodies (those carry customer schema/table/column
  names and secrets).
"""

import atexit
import functools
import inspect
import os
import queue
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import httpx

# Mirror pyproject.toml version.
SERVER_VERSION = "0.1.0"

# Public Datadog client token, committed on purpose. Client tokens are designed to
# be embedded in publicly-distributed code: they can only *submit* logs/RUM and
# cannot read data or manage the org. Override via BIGEYE_TELEMETRY_CLIENT_TOKEN.
DEFAULT_TELEMETRY_CLIENT_TOKEN = "pub9216acb353e273f29e84a7bdcbd3b875"

# Maps a Datadog site to its browser-logs intake host.
_BROWSER_INTAKE_HOSTS = {
    "datadoghq.com": "browser-intake-datadoghq.com",
    "us3.datadoghq.com": "browser-intake-us3-datadoghq.com",
    "us5.datadoghq.com": "browser-intake-us5-datadoghq.com",
    "datadoghq.eu": "browser-intake-datadoghq.eu",
    "ap1.datadoghq.com": "browser-intake-ap1-datadoghq.com",
    "ddog-gov.com": "browser-intake-ddog-gov.com",
}

_SOURCE = "bigeye-mcp-server"
_SERVICE = "bigeye-mcp-server"

# Worker tuning.
_QUEUE_MAXSIZE = 1000
_BATCH_MAX = 50
_FLUSH_INTERVAL_SECS = 5.0
_HTTP_TIMEOUT_SECS = 5.0
_SHUTDOWN_JOIN_SECS = 3.0

_STOP = object()  # sentinel pushed onto the queue to stop the worker


def _detect_runtime() -> str:
    """Best-effort runtime detection (docker vs. local/desktop)."""
    if Path("/.dockerenv").exists() or os.environ.get("container"):
        return "docker"
    return "claude-desktop"


def get_or_create_install_id() -> str:
    """Return a stable anonymous install ID.

    Persisted next to the credentials (Path.home()/.bigeye-mcp), which is
    volume-mounted in docker-compose, so it survives restarts. Falls back to an
    ephemeral in-memory ID if the file can't be read/written.
    """
    try:
        path = Path.home() / ".bigeye-mcp" / "install_id"
        if path.exists():
            existing = path.read_text().strip()
            if existing:
                return existing
        path.parent.mkdir(parents=True, exist_ok=True)
        new_id = uuid.uuid4().hex
        path.write_text(new_id)
        return new_id
    except Exception:
        return uuid.uuid4().hex


def _intake_url(site: str, client_token: str, explicit_url: Optional[str]) -> str:
    """Build the browser-logs intake URL (token passed as a query param)."""
    if explicit_url:
        base = explicit_url
    else:
        host = _BROWSER_INTAKE_HOSTS.get(site, f"browser-intake-{site}")
        base = f"https://{host}/api/v2/logs"
    sep = "&" if "?" in base else "?"
    return (
        f"{base}{sep}dd-api-key={client_token}"
        f"&ddsource={_SOURCE}&dd-evp-origin=bigeye-mcp"
    )


class TelemetryClient:
    """Buffers tool-call events and ships them to Datadog from a daemon thread."""

    def __init__(
        self,
        *,
        enabled: bool,
        client_token: Optional[str],
        site: str,
        install_id: str,
        workspace_id: Optional[int],
        intake_url: Optional[str] = None,
    ):
        self.enabled = bool(enabled and client_token)
        self.install_id = install_id
        self.workspace_id = workspace_id
        self.runtime = _detect_runtime()
        self._url = _intake_url(site, client_token, intake_url) if self.enabled else None
        self._queue: "queue.Queue[Any]" = queue.Queue(maxsize=_QUEUE_MAXSIZE)
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if not self.enabled or self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._worker, name="bigeye-telemetry", daemon=True
        )
        self._thread.start()
        atexit.register(self._shutdown)

    def record_tool_call(
        self,
        name: str,
        duration_ms: float,
        status: str,
        error_type: Optional[str],
    ) -> None:
        """Enqueue a tool-call event. Non-blocking; dropped if the queue is full."""
        if not self.enabled:
            return
        event = {
            "ddsource": _SOURCE,
            "service": _SERVICE,
            "ddtags": f"version:{SERVER_VERSION},tool:{name},status:{status}",
            "message": "mcp.tool.call",
            "tool": name,
            "duration_ms": round(duration_ms, 2),
            "status": status,
            "error_type": error_type,
            "install_id": self.install_id,
            "workspace_id": self.workspace_id,
            "server_version": SERVER_VERSION,
            "runtime": self.runtime,
        }
        try:
            self._queue.put_nowait(event)
        except queue.Full:
            pass

    # -- background worker ---------------------------------------------------

    def _worker(self) -> None:
        client = httpx.Client(timeout=_HTTP_TIMEOUT_SECS)
        try:
            while True:
                batch, stop = self._drain()
                if batch:
                    self._send(client, batch)
                if stop:
                    return
        finally:
            try:
                client.close()
            except Exception:
                pass

    def _drain(self):
        """Block for the next item, then opportunistically pull more (up to a
        batch), returning (events, stop_requested)."""
        batch = []
        stop = False
        try:
            first = self._queue.get(timeout=_FLUSH_INTERVAL_SECS)
            if first is _STOP:
                return batch, True
            batch.append(first)
        except queue.Empty:
            return batch, False
        while len(batch) < _BATCH_MAX:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
            if item is _STOP:
                stop = True
                break
            batch.append(item)
        return batch, stop

    def _send(self, client: httpx.Client, batch) -> None:
        try:
            client.post(
                self._url,
                json=batch,
                headers={"Content-Type": "application/json"},
            )
        except Exception:
            # Telemetry must never surface errors to the caller.
            pass

    def _shutdown(self) -> None:
        if self._thread is None:
            return
        try:
            self._queue.put_nowait(_STOP)
        except queue.Full:
            pass
        self._thread.join(timeout=_SHUTDOWN_JOIN_SECS)


# ---------------------------------------------------------------------------
# Module-level singleton + instrumentation
# ---------------------------------------------------------------------------

_client: Optional[TelemetryClient] = None


def init_telemetry(config: dict) -> TelemetryClient:
    """Create (once) and start the telemetry client from the loaded config."""
    global _client
    if _client is not None:
        return _client

    enabled = config.get("telemetry_enabled", True)
    client_token = (
        os.environ.get("BIGEYE_TELEMETRY_CLIENT_TOKEN")
        or config.get("telemetry_client_token")
        or DEFAULT_TELEMETRY_CLIENT_TOKEN
    )
    site = config.get("telemetry_site") or "datadoghq.com"
    intake_url = os.environ.get("BIGEYE_TELEMETRY_INTAKE_URL") or config.get(
        "telemetry_intake_url"
    )

    _client = TelemetryClient(
        enabled=enabled,
        client_token=client_token,
        site=site,
        # Only generate/persist a tracking ID when telemetry is enabled, so
        # opting out (BIGEYE_TELEMETRY=false) writes nothing to disk.
        install_id=get_or_create_install_id() if enabled else "",
        workspace_id=config.get("workspace_id"),
        intake_url=intake_url,
    )
    _client.start()
    return _client


def _wrap(fn, telemetry: TelemetryClient):
    """Wrap a tool function so each invocation records a telemetry event."""

    def _record(start: float, status: str, error_type: Optional[str]) -> None:
        telemetry.record_tool_call(
            fn.__name__, (time.perf_counter() - start) * 1000, status, error_type
        )

    if inspect.iscoroutinefunction(fn):

        @functools.wraps(fn)
        async def traced(*args, **kwargs):
            start = time.perf_counter()
            status, error_type = "ok", None
            try:
                result = await fn(*args, **kwargs)
                if isinstance(result, dict) and result.get("error"):
                    status, error_type = "error", "tool_error"
                return result
            except Exception as e:
                status, error_type = "error", type(e).__name__
                raise
            finally:
                _record(start, status, error_type)

        return traced

    @functools.wraps(fn)
    def traced(*args, **kwargs):
        start = time.perf_counter()
        status, error_type = "ok", None
        try:
            result = fn(*args, **kwargs)
            if isinstance(result, dict) and result.get("error"):
                status, error_type = "error", "tool_error"
            return result
        except Exception as e:
            status, error_type = "error", type(e).__name__
            raise
        finally:
            _record(start, status, error_type)

    return traced


def instrument_tools(mcp, telemetry: TelemetryClient) -> None:
    """Monkeypatch ``mcp.tool`` so every subsequently-registered tool is timed.

    Must be called immediately after ``FastMCP(...)`` and before any ``@mcp.tool()``
    decorators run. No-op when telemetry is disabled.
    """
    if not telemetry.enabled:
        return

    original_tool = mcp.tool

    def traced_tool(*d_args, **d_kwargs):
        # Support the bare ``@mcp.tool`` form (function passed directly).
        if len(d_args) == 1 and not d_kwargs and callable(d_args[0]):
            fn = d_args[0]
            return original_tool(_wrap(fn, telemetry))

        decorator = original_tool(*d_args, **d_kwargs)

        def register(fn):
            return decorator(_wrap(fn, telemetry))

        return register

    mcp.tool = traced_tool
