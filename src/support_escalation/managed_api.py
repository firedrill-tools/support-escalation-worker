"""Owned bounded local webhook with concurrent authenticated human updates."""

from __future__ import annotations

import asyncio
import hmac
import os
from contextlib import asynccontextmanager
from copy import deepcopy
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.requests import ClientDisconnect

from .domain import event
from .local_runtime import run_event
from .delivery import ENV as PIPE_ENV, config as delivery_config
from .strict import parse_json


class Admission:
    def __init__(self, limit):
        self.limit = limit
        self.active = {}
        self.closing = False
        self.reason = None

    def stop(self, reason="service shutdown"):
        self.closing = True
        self.reason = reason
        for stop in list(self.active.values()):
            stop.set()


def create_managed_app(
    *,
    database,
    event_token,
    allow_writes=False,
    api_key=None,
    model=None,
    base_url=None,
    output_root="artifacts/support-escalation",
    timeout_ms=120000,
    max_active=8,
    action_mode="local",
    pipedream=None,
    allow_pipedream_actions=False,
):
    if (
        type(event_token) is not str
        or not 1 <= len(event_token) <= 256
        or any(ord(c) < 33 or ord(c) > 126 for c in event_token)
    ):
        raise ValueError("explicit bounded EVENT_TOKEN required")
    if (
        type(allow_writes) is not bool
        or type(max_active) is not int
        or not 1 <= max_active <= 8
    ):
        raise ValueError("exact writes and bounded webhook admission required")
    if (
        not isinstance(database, (str, Path))
        or not isinstance(output_root, (str, Path))
        or type(timeout_ms) is not int
        or not 10 <= timeout_ms <= 300000
    ):
        raise ValueError("explicit managed source/output and bounded deadline required")
    state = Admission(max_active)
    if (
        action_mode not in ("local", "pipedream")
        or type(allow_pipedream_actions) is not bool
    ):
        raise ValueError("explicit action mode and permission required")
    if action_mode == "pipedream":
        pipe_values = (
            {k: os.environ.get(v) for k, v in PIPE_ENV.items()}
            if pipedream is None
            else deepcopy(pipedream)
        )
        delivery_config(
            pipe_values,
            allow_actions=allow_pipedream_actions,
            allow_writes=allow_writes,
        )
    else:
        if pipedream is not None or allow_pipedream_actions:
            raise ValueError("local server cannot select remote configuration")
        pipe_values = None
    # Snapshot server configuration. Request JSON cannot choose model/credentials/destination.
    frozen = {
        "database": str(database),
        "allow_writes": allow_writes,
        "api_key": (
            api_key if api_key is not None else os.environ.get("ANTHROPIC_API_KEY", "")
        ),
        "model": model if model is not None else os.environ.get("ANTHROPIC_MODEL", ""),
        "base_url": base_url,
        "output_root": str(output_root),
        "timeout_ms": timeout_ms,
        "action_mode": action_mode,
        "pipedream": pipe_values,
        "allow_pipedream_actions": allow_pipedream_actions,
    }

    @asynccontextmanager
    async def lifespan(app):
        state.closing = False
        state.reason = None
        try:
            yield
        finally:
            state.stop()
            tasks = list(state.active)
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    app = FastAPI(title="Owned synthetic support escalation", lifespan=lifespan)
    app.state.support_admission = state

    @app.get("/health")
    async def health():
        return {
            "status": "closing" if state.closing else "ready",
            "active": len(state.active),
        }

    @app.post("/events")
    async def events(request: Request):
        supplied = request.headers.get("x-event-token")
        if supplied is None or not hmac.compare_digest(
            supplied.encode("utf-8"), event_token.encode("ascii")
        ):
            return JSONResponse({"error": "invalid event token"}, status_code=401)
        if state.closing or len(state.active) >= state.limit:
            return JSONResponse(
                {"error": "support webhook is closing or busy"}, status_code=503
            )
        task = asyncio.current_task()
        stop = asyncio.Event()
        state.active[task] = stop
        disconnect = None
        try:
            chunks = []
            size = 0
            try:
                async with asyncio.timeout(5):
                    async for chunk in request.stream():
                        size += len(chunk)
                        if size > 65536:
                            return JSONResponse(
                                {"error": "event body exceeds64KiB"}, status_code=413
                            )
                        chunks.append(chunk)
                payload = event(parse_json(b"".join(chunks)))
            except (ValueError, UnicodeError, ClientDisconnect):
                return JSONResponse(
                    {"error": "exact original support event JSON required"},
                    status_code=400,
                )
            except TimeoutError:
                return JSONResponse(
                    {"error": "event body deadline exceeded"}, status_code=408
                )

            async def watch_disconnect():
                while not stop.is_set():
                    if await request.is_disconnected():
                        stop.set()
                        return
                    await asyncio.sleep(0.05)

            disconnect = asyncio.create_task(watch_disconnect())
            try:
                result = await run_event(
                    **frozen, event=deepcopy(payload), stop_event=stop
                )
                return JSONResponse(result)
            except Exception as error:
                evidence = getattr(error, "evidence", {})
                return JSONResponse(
                    {
                        "error": str(error),
                        "receipt_path": getattr(error, "receipt_path", None),
                        "business": evidence.get("observed_business"),
                        "acknowledged_effects": evidence.get("acknowledged_effects"),
                        "cleanup_confirmed": evidence.get("cleanup_confirmed"),
                    },
                    status_code=503,
                )
        finally:
            if disconnect is not None:
                disconnect.cancel()
                await asyncio.gather(disconnect, return_exceptions=True)
            state.active.pop(task, None)

    return app
