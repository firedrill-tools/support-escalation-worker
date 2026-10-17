"""Actual ASGI and served HTTP with real model SDK, concurrent updates and shutdown."""

import asyncio
import json
import os
import re
import selectors
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import httpx
import pytest

from support_escalation.managed_api import create_managed_app
from test_native import environment, KEY, MODEL

TOKEN = "synthetic-owned-event-token"
HEADERS = {"x-event-token": TOKEN}


def app_for(options, **extra):
    return create_managed_app(
        database=options["database"],
        event_token=TOKEN,
        api_key=KEY,
        model=MODEL,
        base_url=options["base_url"],
        output_root=options["output_root"],
        **extra,
    )


def test_real_asgi_auth_original_json_and_body_bounds_before_source_or_model(tmp_path):
    with environment(tmp_path) as (source, requests, errors, options):
        app = app_for(options, allow_writes=True)

        async def run():
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://owned-asgi"
                ) as client:
                    assert (
                        await client.post("/events", json=options["event"])
                    ).status_code == 401
                    for raw in [
                        b'{"event_id":"EV-A","event_id":"EV-B"}',
                        b'{"ticket_id":true}',
                        b"\xff",
                    ]:
                        assert (
                            await client.post("/events", content=raw, headers=HEADERS)
                        ).status_code == 400
                    assert (
                        await client.post(
                            "/events", content=b"x" * 65537, headers=HEADERS
                        )
                    ).status_code == 413
                    assert (await client.get("/health")).json() == {
                        "status": "ready",
                        "active": 0,
                    }

        asyncio.run(run())
        assert requests == [] and errors == []
        assert source.snapshot()["records"]["operations"] == []


def test_real_asgi_default_readonly_retains_actual_sdk_preview_without_business_effects(
    tmp_path,
):
    with environment(tmp_path) as (source, requests, errors, options):
        app = app_for(options)

        async def run():
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://owned-asgi"
                ) as client:
                    response = await client.post(
                        "/events", json=options["event"], headers=HEADERS
                    )
                    assert response.status_code == 200, response.text
                    assert response.json()["status"] == "read_only"
                    assert response.json()["cleanup_confirmed"]

        asyncio.run(run())
        assert source.snapshot()["business"]["outbox"] == []
        assert source.snapshot()["business"]["events"] == []
        assert len(requests) == 1 and errors == []


def test_real_concurrent_webhook_reply_changes_source_during_actual_model_http(
    tmp_path,
):
    entered = threading.Event()
    release = threading.Event()

    def held(value, n):
        entered.set()
        assert release.wait(15)
        return value

    with environment(tmp_path, held) as (source, requests, errors, options):
        app = app_for(options, allow_writes=True)

        async def run():
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://owned-asgi"
                ) as client:
                    pending = asyncio.create_task(
                        client.post("/events", json=options["event"], headers=HEADERS)
                    )
                    try:
                        for _ in range(1000):
                            if entered.is_set():
                                break
                            await asyncio.sleep(0.01)
                        assert entered.is_set()
                        duplicate = await client.post(
                            "/events", json=options["event"], headers=HEADERS
                        )
                        assert (
                            duplicate.status_code == 200
                            and duplicate.json()["status"] == "review_required"
                        )
                        changed = {
                            "event_id": "EV-LATE-HTTP",
                            "ticket_id": "T-501",
                            "kind": "agent_reply",
                            "occurred_at": "2026-10-15T12:15:00Z",
                        }
                        reply = await client.post(
                            "/events", json=changed, headers=HEADERS
                        )
                        assert reply.status_code == 200, reply.text
                        assert reply.json()["status"] == "recorded"
                        assert (
                            source.snapshot()["records"]["operations"][0]["status"]
                            == "active"
                        )
                        release.set()
                        done = await asyncio.wait_for(pending, 10)
                        assert done.status_code == 200, done.text
                        assert done.json()["status"] == "changed_before_escalation"
                        assert done.json()["cleanup_confirmed"]
                    finally:
                        release.set()
                        if not pending.done():
                            pending.cancel()
                            await asyncio.gather(pending, return_exceptions=True)

        asyncio.run(run())
        assert source.snapshot()["business"]["outbox"] == []
        assert source.snapshot()["records"]["operations"][0]["status"] == "closed"
        assert len(requests) == 1 and errors == []


def test_real_asgi_admission_and_service_stop_cancel_active_model_then_refuse_new_work(
    tmp_path,
):
    entered = threading.Event()
    release = threading.Event()

    def held(value, n):
        entered.set()
        assert release.wait(15)
        return value

    with environment(tmp_path, held) as (source, requests, errors, options):
        app = app_for(options, allow_writes=True, max_active=1)

        async def run():
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://owned-asgi"
                ) as client:
                    pending = asyncio.create_task(
                        client.post("/events", json=options["event"], headers=HEADERS)
                    )
                    try:
                        for _ in range(1000):
                            if entered.is_set():
                                break
                            await asyncio.sleep(0.01)
                        assert entered.is_set()
                        assert (
                            await client.post(
                                "/events", json=options["event"], headers=HEADERS
                            )
                        ).status_code == 503
                        app.state.support_admission.stop()
                        response = await asyncio.wait_for(pending, 10)
                        assert response.status_code == 503, response.text
                        assert response.json()["cleanup_confirmed"]
                        assert (
                            await client.post(
                                "/events", json=options["event"], headers=HEADERS
                            )
                        ).status_code == 503
                    finally:
                        release.set()
                        if not pending.done():
                            pending.cancel()
                            await asyncio.gather(pending, return_exceptions=True)

        asyncio.run(run())
        assert source.snapshot()["records"]["operations"][0]["status"] == "closed"
        assert source.snapshot()["business"]["outbox"] == []


@contextmanager
def served(tmp_path, options, *, pipedream_settings=None, canonical=False):
    env = {
        k: os.environ[k]
        for k in (
            "PATH",
            "HOME",
            "TMPDIR",
            "TMP",
            "TEMP",
            "LANG",
            "SystemRoot",
            "WINDIR",
        )
        if k in os.environ
    }
    env.update(ANTHROPIC_API_KEY=KEY, EVENT_TOKEN=TOKEN)
    if pipedream_settings is not None:
        from support_escalation.delivery import ENV

        env.update({ENV[k]: v for k, v in pipedream_settings.items() if v is not None})
    args = [
        sys.executable,
        "-m",
        "support_escalation.cli" if canonical else "support_escalation.managed_server",
        "--db",
        options["database"],
        "--port",
        "0",
        "--allow-writes",
        "--model",
        MODEL,
        "--base-url",
        options["base_url"],
        "--output",
        options["output_root"],
    ]
    if pipedream_settings is not None:
        args.extend(["--action-mode", "pipedream", "--allow-pipedream-actions"])
    child = subprocess.Popen(
        args,
        cwd=tmp_path,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    lines = []
    selector = selectors.DefaultSelector()
    selector.register(child.stderr, selectors.EVENT_READ)
    try:
        deadline = time.monotonic() + 15
        origin = None
        while time.monotonic() < deadline and origin is None:
            for key, _ in selector.select(0.1):
                line = os.read(key.fileobj.fileno(), 65536).decode(
                    "utf-8", errors="replace"
                )
                lines.append(line)
                match = re.search(r"http://127\.0\.0\.1:[0-9]+", "".join(lines))
                if match:
                    origin = match.group()
            assert child.poll() is None, "".join(lines)
        assert origin is not None, "".join(lines)
        yield child, origin
    finally:
        selector.close()
        if child.poll() is None:
            child.send_signal(signal.SIGTERM)
        try:
            child.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            child.kill()
            child.communicate(timeout=5)


def test_actual_served_cli_webhook_auth_and_model_queue_outside_checkout(tmp_path):
    with environment(tmp_path) as (source, requests, errors, options):
        with served(tmp_path, options) as (child, origin):
            with httpx.Client(base_url=origin, trust_env=False, timeout=10) as client:
                assert client.post("/events", json=options["event"]).status_code == 401
                response = client.post(
                    "/events", json=options["event"], headers=HEADERS
                )
                assert response.status_code == 200, response.text
                assert response.json()["status"] == "local_queued"
                assert response.json()["cleanup_confirmed"]
                assert client.get("/health").json()["active"] == 0
        assert child.returncode == 0
        assert len(requests) == 1 and errors == []
        assert source.snapshot()["records"]["operations"][0]["status"] == "closed"


@pytest.mark.parametrize("which", [signal.SIGINT, signal.SIGTERM, signal.SIGHUP])
def test_actual_served_cli_signals_stop_admission_then_cancel_owned_model_and_drain(
    tmp_path, which
):
    entered = threading.Event()
    release = threading.Event()

    def held(value, n):
        entered.set()
        assert release.wait(20)
        return value

    with environment(tmp_path, held) as (source, requests, errors, options):
        with served(tmp_path, options) as (child, origin):
            with httpx.Client(base_url=origin, trust_env=False, timeout=15) as client:
                with ThreadPoolExecutor(max_workers=1) as pool:
                    pending = pool.submit(
                        client.post, "/events", json=options["event"], headers=HEADERS
                    )
                    try:
                        assert entered.wait(10)
                        child.send_signal(which)
                        response = pending.result(timeout=10)
                        assert response.status_code == 503, response.text
                        result = response.json()
                        assert result["cleanup_confirmed"]
                        receipt = json.loads(Path(result["receipt_path"]).read_text())
                        assert receipt["status"] == "failed"
                        assert (
                            receipt["cleanup"]["scope_closed"]
                            and receipt["cleanup"]["flow_drained"]
                        )
                        assert source.snapshot()["business"]["outbox"] == []
                        assert (
                            source.snapshot()["records"]["operations"][0]["status"]
                            == "closed"
                        )
                        child.communicate(timeout=10)
                        assert child.returncode == 0
                    finally:
                        release.set()
