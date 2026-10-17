"""Normal local runtime, actual pinned Anthropic SDK and real owned loopback HTTP."""

import asyncio
import base64
import json
import threading
import os
import signal
import subprocess
import sys
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from support_escalation.local_runtime import run_local_event
from support_escalation.managed import ManagedSource

FIXTURE = Path(__file__).parents[1] / "fixtures/starter.json"
KEY = "synthetic-owned-support-key"
MODEL = "claude-synthetic-loopback"
SUMMARY = "Ticket T-501: Cannot access account. Customer unanswered since 2026-10-15T10:00:00Z."


@contextmanager
def environment(tmp_path, mutate=lambda v, n: v):
    requests = []
    errors = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            try:
                assert self.path == "/v1/messages"
                assert self.headers.get("x-api-key") == KEY
                raw = self.rfile.read(int(self.headers["content-length"]))
                requests.append({"body": json.loads(raw), "path": self.path})
                value = {
                    "id": "msg_owned_" + str(len(requests)),
                    "type": "message",
                    "role": "assistant",
                    "model": MODEL,
                    "content": [{"type": "text", "text": SUMMARY}],
                    "stop_reason": "end_turn",
                    "stop_sequence": None,
                    "usage": {"input_tokens": 10, "output_tokens": 2},
                    "original_unknown": {"retained": True},
                }
                value = mutate(value, len(requests))
                raw = value if type(value) is bytes else json.dumps(value).encode()
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
            except (BrokenPipeError, ConnectionResetError):
                pass
            except BaseException as error:
                errors.append(error)
                self.send_response(500)
                self.end_headers()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    source = ManagedSource.seed(tmp_path / "source.db", FIXTURE)
    try:
        yield source, requests, errors, {
            "database": str(source.path),
            "event": {
                "event_id": "EV-CLOCK",
                "ticket_id": "T-501",
                "kind": "clock_tick",
                "occurred_at": "2026-10-15T12:30:00Z",
            },
            "allow_writes": True,
            "api_key": KEY,
            "model": MODEL,
            "base_url": f"http://127.0.0.1:{server.server_port}",
            "output_root": str(tmp_path / "receipts"),
        }
    finally:
        source.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_normal_actual_sdk_completes_before_atomic_local_queue_and_owned_cleanup(
    tmp_path,
):
    with environment(tmp_path) as (source, requests, errors, options):
        result = asyncio.run(run_local_event(**options))
        assert result["status"] == "local_queued"
        assert result["business"]["local_queue_complete"] is True
        assert result["cleanup_confirmed"] is True
        assert result["actual_external_delivery"] is None
        assert len(requests) == 1 and errors == []
        state = source.snapshot()
        assert state["business"]["outbox"][0]["summary"] == SUMMARY
        assert state["records"]["operations"][0]["status"] == "closed"
        receipt = json.loads(Path(result["receipt_path"]).read_text())
        assert receipt["calls"][0]["response"]["original_unknown"] == {"retained": True}
        assert receipt["calls"][0]["reported_usage"]["input_tokens"] == {
            "status": "observed",
            "value": 10,
        }
        assert receipt["sdk_response"]["stop_reason"] == "end_turn"
        assert (
            receipt["calls"][0]["response_base64"]
            == base64.b64encode(
                json.dumps(receipt["calls"][0]["response"]).encode()
            ).decode()
        )
        assert len(receipt["acknowledged_effects"]) == 2
        assert receipt["cleanup"]["source"]["database_closed"]
        assert receipt["cleanup"]["source"]["descriptor_closed"]


@pytest.mark.parametrize(
    "reason", ["max_tokens", None, "missing", "pause_turn", "tool_use"]
)
def test_normal_original_incomplete_reason_stops_before_queue_with_original_evidence(
    tmp_path, reason
):
    def mutate(value, n):
        if reason == "missing":
            value.pop("stop_reason")
        else:
            value["stop_reason"] = reason
        return value

    with environment(tmp_path, mutate) as (source, requests, errors, options):
        with pytest.raises(ValueError, match="end_turn") as caught:
            asyncio.run(run_local_event(**options))
        evidence = caught.value.evidence
        assert evidence["status"] == "failed"
        assert evidence["cleanup_confirmed"] is True
        assert evidence["observed_business"]["local_queue_complete"] is False
        assert len(evidence["acknowledged_effects"]) == 1
        assert source.snapshot()["business"]["outbox"] == []
        assert source.snapshot()["records"]["operations"][0]["status"] == "closed"
        assert evidence["calls"][0]["response_complete"] is True
        assert evidence["calls"][0]["response"]["original_unknown"] == {
            "retained": True
        }
        assert evidence["calls"][0]["reported_usage"]["input_tokens"] == {
            "status": "observed",
            "value": 10,
        }
        assert "sdk_response" not in evidence
        assert len(requests) == 1 and errors == []


def test_normal_missing_key_prepares_receipt_before_source_scope_and_provider(tmp_path):
    with environment(tmp_path) as (source, requests, errors, options):
        with pytest.raises(ValueError, match="ANTHROPIC_API_KEY") as caught:
            asyncio.run(run_local_event(**{**options, "api_key": ""}))
        evidence = caught.value.evidence
        assert evidence["status"] == "failed" and evidence["cleanup_confirmed"]
        assert evidence["allocation"] == {
            "source": False,
            "scope": False,
            "provider": False,
        }
        assert evidence["source_attempted"] is False
        assert source.snapshot()["records"]["operations"] == []
        assert requests == [] and errors == []


def cli_args(options):
    return [
        sys.executable,
        "-m",
        "support_escalation.local_cli",
        "--db",
        options["database"],
        "--event-json",
        json.dumps(options["event"]),
        "--allow-writes",
        "--model",
        MODEL,
        "--base-url",
        options["base_url"],
        "--output",
        options["output_root"],
    ]


def cli_env():
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
    return {**env, "ANTHROPIC_API_KEY": KEY}


def test_actual_normal_local_cli_runs_from_outside_checkout_and_exits_after_cleanup(
    tmp_path,
):
    with environment(tmp_path) as (source, requests, errors, options):
        p = subprocess.run(
            cli_args(options),
            cwd=tmp_path,
            env=cli_env(),
            capture_output=True,
            text=True,
            timeout=20,
        )
        assert p.returncode == 0, p.stdout + p.stderr
        result = json.loads(p.stdout)
        assert result["status"] == "local_queued" and result["cleanup_confirmed"]
        assert len(requests) == 1 and errors == []
        assert source.snapshot()["records"]["operations"][0]["status"] == "closed"


@pytest.mark.parametrize("which", [signal.SIGINT, signal.SIGTERM, signal.SIGHUP])
def test_actual_normal_cli_signals_revoke_before_aborting_held_provider_http(
    tmp_path, which
):
    entered = threading.Event()
    release = threading.Event()

    def held(value, n):
        entered.set()
        assert release.wait(15)
        return value

    with environment(tmp_path, held) as (source, requests, errors, options):
        child = subprocess.Popen(
            cli_args(options),
            cwd=tmp_path,
            env=cli_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            assert entered.wait(10)
            child.send_signal(which)
            stdout, stderr = child.communicate(timeout=10)
            assert child.returncode == 1, stdout + stderr
            result = json.loads(stdout)
            assert result["observed_signal"] == which.name
            assert result["cleanup_confirmed"] is True
            evidence = json.loads(Path(result["receipt_path"]).read_text())
            assert evidence["status"] == "failed"
            kinds = [r["kind"] for r in evidence["lifecycle"]]
            assert kinds.index("scope_close_returned") < kinds.index(
                "flow_cancel_requested"
            )
            assert (
                next(
                    r
                    for r in evidence["lifecycle"]
                    if r["kind"] == "flow_cancel_requested"
                )["scope_closed"]
                is True
            )
            assert evidence["cleanup"]["flow_drained"] is True
            assert evidence["cleanup"]["provider_close_confirmed"] is True
            assert len(evidence["acknowledged_effects"]) == 1
            assert source.snapshot()["business"]["outbox"] == []
            assert source.snapshot()["records"]["operations"][0]["status"] == "closed"
        finally:
            release.set()
            if child.poll() is None:
                child.kill()
            child.communicate(timeout=10)


@pytest.mark.parametrize("kind", ["caller", "parent", "deadline"])
def test_actual_caller_parent_and_deadline_cancel_owned_http_and_preserve_prior_event(
    tmp_path, kind
):
    entered = threading.Event()
    release = threading.Event()

    def held(value, n):
        entered.set()
        assert release.wait(15)
        return value

    with environment(tmp_path, held) as (source, requests, errors, options):

        async def run():
            stop = asyncio.Event()
            if kind == "deadline":
                options["timeout_ms"] = 1000
            else:
                options["stop_event" if kind == "caller" else "parent_event"] = stop
            task = asyncio.create_task(run_local_event(**options))
            try:
                deadline = asyncio.get_running_loop().time() + 10
                while not entered.is_set():
                    assert asyncio.get_running_loop().time() < deadline
                    await asyncio.sleep(0.01)
                if kind != "deadline":
                    stop.set()
                with pytest.raises((RuntimeError, TimeoutError)) as caught:
                    await asyncio.wait_for(task, 5)
                assert caught.value.evidence["cleanup_confirmed"]
                assert caught.value.evidence["cleanup"]["scope_closed"]
                assert caught.value.evidence["cleanup"]["flow_drained"]
                assert source.snapshot()["business"]["outbox"] == []
                assert len(caught.value.evidence["acknowledged_effects"]) == 1
            finally:
                release.set()
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

        asyncio.run(run())


def test_direct_task_cancellation_is_shielded_until_scope_revocation_then_owned_drain(
    tmp_path,
):
    entered = threading.Event()
    release = threading.Event()

    def held(value, n):
        entered.set()
        assert release.wait(15)
        return value

    with environment(tmp_path, held) as (source, requests, errors, options):

        async def run():
            task = asyncio.create_task(run_local_event(**options))
            try:
                for _ in range(1000):
                    if entered.is_set():
                        break
                    await asyncio.sleep(0.01)
                assert entered.is_set()
                task.cancel()
                with pytest.raises(RuntimeError, match="cancelled") as caught:
                    await asyncio.wait_for(task, 5)
                assert caught.value.evidence["cleanup_confirmed"]
                kinds = [r["kind"] for r in caught.value.evidence["lifecycle"]]
                assert kinds.index("scope_close_returned") < kinds.index(
                    "flow_cancel_requested"
                )
                assert (
                    source.snapshot()["records"]["operations"][0]["status"] == "closed"
                )
            finally:
                release.set()
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

        asyncio.run(run())


def test_readonly_actual_sdk_draft_is_retained_without_event_or_queue_effects(tmp_path):
    with environment(tmp_path) as (source, requests, errors, options):
        result = asyncio.run(run_local_event(**{**options, "allow_writes": False}))
        assert result["status"] == "read_only" and result["summary"] == SUMMARY
        assert result["business"]["local_queue_complete"] is False
        current = source.snapshot()
        assert (
            current["business"]["events"] == [] and current["business"]["outbox"] == []
        )
        receipt = json.loads(Path(result["receipt_path"]).read_text())
        assert receipt["acknowledged_effects"] == []
        assert len(receipt["acknowledged_decisions"]) == 2
        assert all(
            d["decision"]["status"] == "denied"
            for d in receipt["acknowledged_decisions"]
        )
        assert len(requests) == 1


@pytest.mark.parametrize("kind", ["agent_reply", "resolved"])
def test_actual_independent_late_change_during_http_blocks_old_summary_queue(
    tmp_path, kind
):
    def changed(value, n):
        with ManagedSource(tmp_path / "source.db") as source:
            source.event(
                {
                    "event_id": "EV-LATE",
                    "ticket_id": "T-501",
                    "kind": kind,
                    "occurred_at": "2026-10-15T12:15:00Z",
                }
            )
        return value

    with environment(tmp_path, changed) as (source, requests, errors, options):
        result = asyncio.run(run_local_event(**options))
        assert result["status"] == "changed_before_escalation"
        assert source.snapshot()["business"]["outbox"] == []
        assert result["cleanup_confirmed"] and len(requests) == 1 and errors == []
        receipt = json.loads(Path(result["receipt_path"]).read_text())
        assert "changed_during_draft" in receipt


def test_already_recorded_clock_is_inspected_without_replaying_model_or_queue(tmp_path):
    with environment(tmp_path) as (source, requests, errors, options):
        first = asyncio.run(run_local_event(**options))
        second = asyncio.run(run_local_event(**options))
        assert (
            first["status"] == "local_queued" and second["status"] == "already_recorded"
        )
        assert len(requests) == 1 and errors == []
        assert len(source.snapshot()["business"]["outbox"]) == 1
        assert len(source.snapshot()["records"]["operations"]) == 1
