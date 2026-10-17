"""Actual normal Anthropic + Pipedream SDKs over two independently owned HTTP servers."""

import asyncio
import json
import threading
import os
import signal
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
import httpx

from support_escalation.local_runtime import run_event
from support_escalation.managed import ManagedSource
from test_native import environment

TOKEN = "synthetic-owned-pipedream-token"
OAUTH_TOKEN = "synthetic-owned-oauth-returned-token"
SECRET = "synthetic-owned-client-secret"


@contextmanager
def pipedream(oauth=False, mutate=lambda body, n: body):
    requests = []
    errors = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            try:
                raw = self.rfile.read(int(self.headers["content-length"]))
                value = json.loads(raw)
                assert self.headers.get("x-pd-environment") == "development"
                requests.append({"path": self.path, "body": value})
                if self.path == "/v1/oauth/token":
                    assert oauth
                    assert value == {
                        "client_id": "synthetic-owned-client",
                        "client_secret": SECRET,
                        "grant_type": "client_credentials",
                    }
                    assert self.headers.get("authorization") is None
                    result = {
                        "access_token": OAUTH_TOKEN,
                        "token_type": "Bearer",
                        "expires_in": 3600,
                        "original_unknown": {"retained": True},
                    }
                else:
                    assert self.path == "/v1/connect/proj_owned_synthetic/actions/run"
                    assert self.headers.get("authorization") == "Bearer " + (
                        OAUTH_TOKEN if oauth else TOKEN
                    )
                    assert value["id"] == "slack_v2-post-message"
                    assert value["external_user_id"] == "synthetic-external-user"
                    assert value["configured_props"]["slack_v2"] == {
                        "authProvisionId": "apn_owned_synthetic"
                    }
                    assert value["configured_props"]["channel"] == "C00000001"
                    assert value["configured_props"]["text"].startswith(
                        "Support escalation T-501 (EV-CLOCK): "
                    )
                    result = {
                        "ret": {
                            "ok": True,
                            "channel": "C00000001",
                            "ts": "1792195200.000001",
                        },
                        "exports": {},
                        "os": [],
                        "original_unknown": {"retained": True},
                    }
                    result = mutate(result, len(requests))
                status, extra = 200, 0
                if type(result) is tuple:
                    status, result, extra = result
                body = result if type(result) is bytes else json.dumps(result).encode()
                self.send_response(status)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(body) + extra))
                if extra:
                    self.close_connection = True
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass
            except BaseException as error:
                errors.append(error)
                self.send_response(500)
                self.end_headers()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    settings = {
        "project_id": "proj_owned_synthetic",
        "external_user_id": "synthetic-external-user",
        "auth_provision_id": "apn_owned_synthetic",
        "channel": "C00000001",
        "base_url": f"http://127.0.0.1:{server.server_port}",
        "project_environment": "development",
    }
    if oauth:
        settings.update(client_id="synthetic-owned-client", client_secret=SECRET)
    else:
        settings["access_token"] = TOKEN
    try:
        yield settings, requests, errors
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.mark.parametrize("oauth", [False, True])
def test_normal_two_actual_sdks_acknowledge_exact_destination_without_delivery_claim_or_secret_leak(
    tmp_path, oauth
):
    with environment(tmp_path) as (source, model_requests, model_errors, options):
        with pipedream(oauth) as (settings, requests, errors):
            result = asyncio.run(
                run_event(
                    **options,
                    action_mode="pipedream",
                    pipedream=settings,
                    allow_pipedream_actions=True,
                )
            )
            assert result["status"] == "remote_acknowledged"
            assert result["business"]["workflow_complete"]
            assert result["actual_external_delivery"] is None
            assert result["cleanup_confirmed"]
            assert len(model_requests) == 1 and model_errors == []
            assert len(requests) == (2 if oauth else 1) and errors == []
            final = source.snapshot()
            assert final["records"]["operations"][0]["status"] == "closed"
            outbox = final["business"]["outbox"][0]
            assert outbox["destination"]["channel"] == "C00000001"
            assert outbox["acknowledgement"]["kind"] == "owned_sdk_action_observation"
            assert outbox["acknowledgement"]["actual_delivery"] is None
            receipt = json.loads(Path(result["receipt_path"]).read_text())
            encoded = json.dumps(receipt)
            for secret in (TOKEN, OAUTH_TOKEN, SECRET):
                assert secret not in encoded
            assert len(receipt["remote_observations"]) == 1
            assert receipt["remote_observations"][0]["reported_channel"] == "C00000001"
            assert receipt["delivery_sdk_response"]["ret"]["ok"] is True
            assert receipt["cleanup"]["delivery_close_confirmed"]
            assert len(receipt["acknowledged_effects"]) == 3
            if oauth:
                auth = receipt["delivery_calls"][0]
                assert (
                    auth["request_credentials_redacted"]
                    and auth["response_credentials_redacted"]
                )
                assert "response_base64" not in auth and "request_base64" not in auth
                assert auth["response"]["original_unknown"] == {"retained": True}


def remote_options(options, settings, **changes):
    return {
        **options,
        "action_mode": "pipedream",
        "pipedream": settings,
        "allow_pipedream_actions": True,
        **changes,
    }


@pytest.mark.parametrize(
    "fault",
    [
        "false",
        "empty",
        "error",
        "wrong_channel",
        "string_ok",
        "missing_ts",
        "duplicate",
        "oversized",
        "http500",
        "partial",
    ],
)
def test_actual_original_action_failure_marks_uncertainty_once_before_revocation_and_never_replays(
    tmp_path, fault
):
    def failed(value, n):
        if fault == "false":
            value["ret"] = {"ok": False, "error": "synthetic_rejection"}
        elif fault == "empty":
            value = {}
        elif fault == "error":
            value = {"error": {"message": "synthetic action failure"}}
        elif fault == "wrong_channel":
            value["ret"]["channel"] = "C00000002"
        elif fault == "string_ok":
            value["ret"]["ok"] = "true"
        elif fault == "missing_ts":
            value["ret"].pop("ts")
        elif fault == "duplicate":
            return (
                json.dumps(value)
                .replace('"ok": true', '"ok": false, "ok": true')
                .encode()
            )
        elif fault == "oversized":
            value["original_unknown"] = {"oversized": "x" * (1024 * 1024)}
        elif fault == "http500":
            return 500, {"error": "owned synthetic internal action error"}, 0
        elif fault == "partial":
            return 200, value, 20
        return value

    with environment(tmp_path) as (source, model_requests, model_errors, options):
        with pipedream(mutate=failed) as (settings, requests, errors):
            args = remote_options(options, settings)
            with pytest.raises(Exception) as caught:
                asyncio.run(run_event(**args))
            evidence = caught.value.evidence
            assert evidence["status"] == "failed" and evidence["cleanup_confirmed"]
            assert (
                evidence["observed_business"]["notification_state"] == "review_required"
            )
            assert evidence["observed_business"]["workflow_complete"] is False
            assert evidence["observed_business"]["actual_external_delivery"] is None
            assert len(evidence["acknowledged_effects"]) == 3
            assert (
                evidence["actions"][-1]["kind"] == "owned_uncertainty_before_revocation"
            )
            current = source.snapshot()
            assert (
                current["business"]["outbox"][0]["acknowledgement"]["kind"]
                == "control_uncertainty_observation"
            )
            assert current["records"]["operations"][0]["status"] == "closed"
            repeated = asyncio.run(run_event(**args))
            assert repeated["status"] == "review_required"
            later = asyncio.run(
                run_event(**{**args, "event": {**args["event"], "event_id": "EV-NEXT"}})
            )
            assert later["status"] == "not_due_or_resolved"
            assert len(model_requests) == 1 and model_errors == []
            assert len(requests) == 1 and errors == []
            if fault == "http500":
                assert evidence["delivery_calls"][0]["status_code"] == 500


@pytest.mark.parametrize("gate", ["actions", "writes", "development"])
def test_explicit_remote_permissions_and_development_mode_refuse_before_any_allocation(
    tmp_path, gate
):
    with environment(tmp_path) as (source, model_requests, model_errors, options):
        with pipedream() as (settings, requests, errors):
            args = remote_options(options, settings)
            if gate == "actions":
                args["allow_pipedream_actions"] = False
            elif gate == "writes":
                args["allow_writes"] = False
            else:
                settings["project_environment"] = "production"
            with pytest.raises(ValueError) as caught:
                asyncio.run(run_event(**args))
            assert caught.value.evidence["allocation"] == {
                "source": False,
                "scope": False,
                "provider": False,
            }
            assert caught.value.evidence["source_attempted"] is False
            assert source.snapshot()["records"]["operations"] == []
            assert model_requests == [] and requests == []


def test_frozen_complete_destination_and_credential_cannot_change_during_model_await(
    tmp_path,
):
    with pipedream() as (settings, requests, errors):

        def mutate(value, n):
            settings["channel"] = "C00000002"
            settings["access_token"] = "changed-after-native-start"
            return value

        with environment(tmp_path, mutate) as (
            source,
            model_requests,
            model_errors,
            options,
        ):
            result = asyncio.run(run_event(**remote_options(options, settings)))
            assert result["status"] == "remote_acknowledged" and errors == []
            assert requests[0]["body"]["configured_props"]["channel"] == "C00000001"
            assert (
                source.snapshot()["records"]["operations"][0]["destination"]["channel"]
                == "C00000001"
            )


@pytest.mark.parametrize("kind", ["caller", "deadline"])
def test_actual_remote_cancellation_persists_review_and_revokes_before_aborting_action_http(
    tmp_path, kind
):
    entered = threading.Event()
    release = threading.Event()

    def held(value, n):
        entered.set()
        assert release.wait(15)
        return value

    with environment(tmp_path) as (source, model_requests, model_errors, options):
        with pipedream(mutate=held) as (settings, requests, errors):

            async def run():
                stop = asyncio.Event()
                args = remote_options(options, settings)
                if kind == "caller":
                    args["stop_event"] = stop
                else:
                    args["timeout_ms"] = 1000
                task = asyncio.create_task(run_event(**args))
                try:
                    for _ in range(1000):
                        if entered.is_set():
                            break
                        await asyncio.sleep(0.01)
                    assert entered.is_set()
                    if kind == "caller":
                        stop.set()
                    with pytest.raises((RuntimeError, TimeoutError)) as caught:
                        await asyncio.wait_for(task, 5)
                    evidence = caught.value.evidence
                    assert evidence["cleanup_confirmed"]
                    assert (
                        evidence["observed_business"]["notification_state"]
                        == "review_required"
                    )
                    assert evidence["cleanup"]["delivery_close_confirmed"]
                    kinds = [r["kind"] for r in evidence["lifecycle"]]
                    assert kinds.index("scope_close_returned") < kinds.index(
                        "flow_cancel_requested"
                    )
                    assert (
                        source.snapshot()["business"]["outbox"][0]["status"]
                        == "review_required"
                    )
                    assert (
                        source.snapshot()["records"]["operations"][0]["status"]
                        == "closed"
                    )
                finally:
                    release.set()
                    if not task.done():
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)

            asyncio.run(run())
            assert len(requests) == 1 and len(model_requests) == 1


def test_current_ticket_change_during_action_retains_original_ack_but_requires_review(
    tmp_path,
):
    def changed(value, n):
        with ManagedSource(tmp_path / "source.db") as source:
            source.event(
                {
                    "event_id": "EV-ACTION-REPLY",
                    "ticket_id": "T-501",
                    "kind": "agent_reply",
                    "occurred_at": "2026-10-15T12:15:00Z",
                }
            )
        return value

    with environment(tmp_path) as (source, model_requests, model_errors, options):
        with pipedream(mutate=changed) as (settings, requests, errors):
            with pytest.raises(ValueError, match="changed") as caught:
                asyncio.run(run_event(**remote_options(options, settings)))
            evidence = caught.value.evidence
            assert len(evidence["remote_observations"]) == 1
            assert evidence["remote_observations"][0]["status"] == "remote_acknowledged"
            assert (
                evidence["observed_business"]["notification_state"] == "review_required"
            )
            assert evidence["cleanup_confirmed"]
            assert (
                source.snapshot()["business"]["tickets"][0]["last_agent_at"]
                == "2026-10-15T12:15:00Z"
            )
            assert len(requests) == 1 and errors == []


def test_actual_owned_asgi_remote_mode_uses_both_sdks_and_fixed_server_destination(
    tmp_path,
):
    from support_escalation.managed_api import create_managed_app
    from test_managed_api import TOKEN as EVENT_TOKEN, HEADERS

    with environment(tmp_path) as (source, model_requests, model_errors, options):
        with pipedream() as (settings, requests, errors):
            app = create_managed_app(
                database=options["database"],
                event_token=EVENT_TOKEN,
                allow_writes=True,
                api_key=options["api_key"],
                model=options["model"],
                base_url=options["base_url"],
                output_root=options["output_root"],
                action_mode="pipedream",
                pipedream=settings,
                allow_pipedream_actions=True,
            )

            async def run():
                async with app.router.lifespan_context(app):
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=app),
                        base_url="http://owned-asgi",
                    ) as client:
                        denied = await client.post(
                            "/events",
                            json={
                                **options["event"],
                                "pipedream": {"channel": "C00000002"},
                            },
                            headers=HEADERS,
                        )
                        assert (
                            denied.status_code == 400
                            and requests == []
                            and model_requests == []
                        )
                        response = await client.post(
                            "/events", json=options["event"], headers=HEADERS
                        )
                        assert response.status_code == 200, response.text
                        assert response.json()["status"] == "remote_acknowledged"
                        assert response.json()["actual_external_delivery"] is None

            asyncio.run(run())
            assert len(requests) == 1 and len(model_requests) == 1 and errors == []


def test_canonical_owned_serving_cli_dispatches_actual_two_sdk_path_outside_checkout(
    tmp_path,
):
    from test_managed_api import served, HEADERS

    with environment(tmp_path) as (source, model_requests, model_errors, options):
        with pipedream() as (settings, requests, errors):
            with served(
                tmp_path, options, pipedream_settings=settings, canonical=True
            ) as (child, origin):
                with httpx.Client(
                    base_url=origin, trust_env=False, timeout=15
                ) as client:
                    response = client.post(
                        "/events", json=options["event"], headers=HEADERS
                    )
                    assert response.status_code == 200, response.text
                    assert response.json()["status"] == "remote_acknowledged"
                    assert response.json()["cleanup_confirmed"]
            assert child.returncode == 0
            assert len(requests) == 1 and len(model_requests) == 1 and errors == []


def test_canonical_serve_sigterm_during_actual_action_persists_review_before_draining(
    tmp_path,
):
    from test_managed_api import served, HEADERS

    entered = threading.Event()
    release = threading.Event()

    def held(value, n):
        entered.set()
        assert release.wait(20)
        return value

    with environment(tmp_path) as (source, model_requests, model_errors, options):
        with pipedream(mutate=held) as (settings, requests, errors):
            with served(
                tmp_path, options, pipedream_settings=settings, canonical=True
            ) as (child, origin):
                with httpx.Client(
                    base_url=origin, trust_env=False, timeout=15
                ) as client:
                    with ThreadPoolExecutor(max_workers=1) as pool:
                        pending = pool.submit(
                            client.post,
                            "/events",
                            json=options["event"],
                            headers=HEADERS,
                        )
                        try:
                            assert entered.wait(10)
                            child.send_signal(signal.SIGTERM)
                            response = pending.result(timeout=10)
                            assert response.status_code == 503, response.text
                            result = response.json()
                            assert result["cleanup_confirmed"]
                            assert (
                                result["business"]["notification_state"]
                                == "review_required"
                            )
                            child.communicate(timeout=10)
                            assert child.returncode == 0
                        finally:
                            release.set()
            assert (
                source.snapshot()["business"]["outbox"][0]["status"]
                == "review_required"
            )
            assert source.snapshot()["records"]["operations"][0]["status"] == "closed"
            assert len(requests) == 1


def test_canonical_seed_is_exclusive_model_free_and_closes_real_managed_source(
    tmp_path,
):
    path = tmp_path / "canonical.db"
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
    args = [sys.executable, "-m", "support_escalation.cli", "--seed", "--db", str(path)]
    result = subprocess.run(
        args, cwd=tmp_path, env=env, text=True, capture_output=True, timeout=15
    )
    assert result.returncode == 0, result.stdout + result.stderr
    info = json.loads(result.stdout)
    assert info["initialized"] and info["source_version"] == 2
    with ManagedSource(path) as source:
        assert source.snapshot()["records"]["operations"] == []
    before = path.read_bytes()
    repeated = subprocess.run(
        args, cwd=tmp_path, env=env, text=True, capture_output=True, timeout=15
    )
    assert repeated.returncode != 0 and path.read_bytes() == before


def test_actual_delivery_close_ack_failure_preserves_remote_ack_and_never_retries(
    tmp_path, monkeypatch
):
    from support_escalation.delivery import Delivery

    original = Delivery.close
    closed = []

    async def failed(self):
        closed.append(self)
        await original(self)
        raise RuntimeError("actual Pipedream close acknowledgement failed")

    monkeypatch.setattr(Delivery, "close", failed)
    with environment(tmp_path) as (source, model_requests, model_errors, options):
        with pipedream() as (settings, requests, errors):
            with pytest.raises(RuntimeError, match="Pipedream close") as caught:
                asyncio.run(run_event(**remote_options(options, settings)))
            evidence = caught.value.evidence
            assert (
                evidence["status"] == "failed"
                and evidence["cleanup_confirmed"] is False
            )
            assert (
                evidence["cleanup"]["delivery_closed"]
                and not evidence["cleanup"]["delivery_close_confirmed"]
            )
            assert (
                evidence["observed_business"]["notification_state"]
                == "remote_acknowledged"
            )
            assert evidence["observed_business"]["workflow_complete"]
            assert len(closed) == 1 and len(requests) == 1
            assert source.snapshot()["records"]["operations"][0]["status"] == "closed"


def test_actual_recording_loss_after_remote_source_ack_retains_both_sdk_and_source_effects(
    tmp_path, monkeypatch
):
    from support_escalation.evidence import Receipt

    original = Receipt.save
    parked = tmp_path / "parked-remote-receipt"
    moved = []

    def failed(self):
        if not moved and any(
            d["result"]["status"] == "remote_acknowledged"
            for d in self.data["acknowledged_effects"]
        ):
            self.path.parent.rename(parked)
            moved.append(True)
        return original(self)

    monkeypatch.setattr(Receipt, "save", failed)
    with environment(tmp_path) as (source, model_requests, model_errors, options):
        with pipedream() as (settings, requests, errors):
            with pytest.raises(FileNotFoundError) as caught:
                asyncio.run(run_event(**remote_options(options, settings)))
            evidence = caught.value.evidence
            assert (
                evidence["recording_status"] == "unknown"
                and evidence["status"] == "failed"
            )
            assert (
                evidence["observed_business"]["notification_state"]
                == "remote_acknowledged"
            )
            assert evidence["cleanup_confirmed"]
            assert (
                len(evidence["acknowledged_effects"]) == 3
                and len(evidence["remote_observations"]) == 1
            )
            durable = json.loads((parked / "receipt.json").read_text())
            assert len(durable["acknowledged_effects"]) == 2
            assert len(durable["remote_observations"]) == 1
            assert (
                source.snapshot()["business"]["outbox"][0]["status"]
                == "remote_acknowledged"
            )
            assert len(requests) == 1 and len(model_requests) == 1
