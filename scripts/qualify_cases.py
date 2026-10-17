"""Installed production probe: real SDKs, owned HTTP, private cases and preserving resets."""

import argparse
import asyncio
import json
import os
import subprocess
import sys
import tempfile
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from support_escalation.local_runtime import run_event
from support_escalation.managed import ManagedSource
from support_escalation.populations import dataset, definition
from support_escalation.strict import sha256

MODEL = "claude-owned-population-probe"
KEY = "synthetic-owned-population-key"
TOKEN = "synthetic-owned-population-action-token"


def cli(*args):
    value = subprocess.run(
        [sys.executable, "-I", "-m", "support_escalation.cases", *map(str, args)],
        check=True,
        capture_output=True,
        text=True,
        timeout=40,
        cwd=tempfile.gettempdir(),
    )
    return json.loads(value.stdout)


def files(root):
    return {
        str(p.relative_to(root)): (p.stat().st_mode & 0o777, sha256(p.read_bytes()))
        for p in root.rglob("*")
        if p.is_file()
    }


@contextmanager
def endpoints(database, during):
    requests, actions, errors = [], [], []
    updated = False

    def update(phase):
        nonlocal updated
        if during is not None and during["phase"] == phase:
            assert not updated
            with ManagedSource(database) as source:
                result = source.event(during["event"])
                assert result["result"]["status"] == "recorded"
            updated = True

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            try:
                body = json.loads(self.rfile.read(int(self.headers["content-length"])))
                if self.path == "/v1/messages":
                    assert self.headers.get("x-api-key") == KEY
                    assert self.headers.get("anthropic-version") == "2023-06-01"
                    requests.append(body)
                    public = json.loads(body["messages"][0]["content"])
                    row = public["ticket"]
                    # Derive the synthetic HTTP response solely from the actual
                    # public SDK request. Private expected outcomes are not a model.
                    summary = f"Ticket {row['id']}: {row['subject']}. Customer unanswered since {row['last_customer_at']}."
                    update("model")
                    result = {
                        "id": "msg_owned_" + str(len(requests)),
                        "type": "message",
                        "role": "assistant",
                        "model": MODEL,
                        "content": [{"type": "text", "text": summary}],
                        "stop_reason": "end_turn",
                        "stop_sequence": None,
                        "usage": {"input_tokens": 10, "output_tokens": 2},
                        "original_unknown": {"owned_http": True},
                    }
                else:
                    assert self.path == "/v1/connect/proj_owned_population/actions/run"
                    assert self.headers.get("authorization") == "Bearer " + TOKEN
                    assert self.headers.get("x-pd-environment") == "development"
                    assert body["id"] == "slack_v2-post-message"
                    assert body["external_user_id"] == "synthetic-support-user"
                    assert body["configured_props"]["channel"] == "C00000001"
                    assert body["configured_props"]["slack_v2"] == {
                        "authProvisionId": "apn_owned_population"
                    }
                    actions.append(body)
                    update("action")
                    result = {
                        "ret": {
                            "ok": True,
                            "channel": "C00000001",
                            "ts": "1792195200.000001",
                        },
                        "exports": {},
                        "os": [],
                        "original_unknown": {"owned_http": True},
                    }
                raw = json.dumps(result).encode()
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
            except BaseException as error:
                errors.append(repr(error))
                self.send_response(500)
                self.end_headers()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requests, actions, errors
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()


def qualify(root):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    listed = cli("list")
    assert len(listed["cases"]) == 12
    results = []
    for item in listed["cases"]:
        case = definition(item["id"])
        original = root / case["id"]
        initial = cli("init", "--case", case["id"], "--root", original)
        assert (
            cli("inspect", "--root", original)["source_sha256"]
            == initial["source_sha256"]
        )
        assert all(
            initial["source"]["records"][k] == []
            for k in ("operations", "observations", "decisions")
        )
        database = initial["database"]
        with endpoints(database, case["during"]) as (base, requests, actions, errors):
            statuses = []
            for payload in case["events"]:
                options = dict(
                    database=database,
                    event=payload,
                    allow_writes=True,
                    api_key=KEY,
                    model=MODEL,
                    base_url=base,
                    output_root=str(original / "receipts"),
                    timeout_ms=30000,
                    action_mode=case["mode"],
                )
                if case["mode"] == "pipedream":
                    options.update(
                        allow_pipedream_actions=True,
                        pipedream={
                            "base_url": base,
                            "project_environment": "development",
                            "project_id": "proj_owned_population",
                            "external_user_id": "synthetic-support-user",
                            "auth_provision_id": "apn_owned_population",
                            "channel": "C00000001",
                            "access_token": TOKEN,
                        },
                    )
                try:
                    result = asyncio.run(run_event(**options))
                    status = result["status"]
                    assert result["actual_external_delivery"] is None
                    receipt = json.loads(Path(result["receipt_path"]).read_text())
                except ValueError as error:
                    assert (
                        case["during"] is not None
                        and case["during"]["phase"] == "action"
                    )
                    receipt = error.evidence
                    assert json.loads(json.dumps(receipt)) == json.loads(
                        Path(error.receipt_path).read_text()
                    )
                    assert receipt["status"] == "failed"
                    status = "failed"
                statuses.append(status)
                assert receipt["cleanup_confirmed"]
                if status == "failed":
                    assert len(receipt["remote_observations"]) == 1
                    assert (
                        receipt["remote_observations"][0]["reported_channel"]
                        == "C00000001"
                    )
                assert receipt["recording_status"] == "durable"
            assert errors == [], errors
            expected = case["reference"]
            assert statuses == expected["statuses"], (case["id"], statuses, expected)
            assert len(requests) == expected["model_requests"]
            assert len(actions) == expected["action_requests"]
            for request in requests:
                encoded = json.dumps(request)
                for private in (
                    case["id"],
                    "support-populations",
                    "definition_sha256",
                    "dataset_sha256",
                    "private_reference",
                    "prior_human_steps",
                    TOKEN,
                ):
                    assert private not in encoded, private
                public = json.loads(request["messages"][0]["content"])
                assert set(public) == {
                    "ticket",
                    "event",
                    "eligible",
                    "previous_event_status",
                }
            if case["after"] is not None:
                with ManagedSource(database) as source:
                    before = source.snapshot()
                    try:
                        source.event(case["after"]["event"])
                    except ValueError as error:
                        assert str(error) == case["after"]["error"]
                    else:
                        raise AssertionError("exhausted revision was accepted")
                    assert source.snapshot()["sha256"] == before["sha256"]
            final = cli("inspect", "--root", original)
            assert all(
                o["status"] == "closed"
                for o in final["source"]["records"]["operations"]
            )
            selected = {p["ticket_id"] for p in case["events"]}
            for old in initial["source"]["business"]["tickets"]:
                if old["id"] not in selected:
                    assert old in final["source"]["business"]["tickets"]
            assert (
                final["source"]["business"]["outbox"][: initial["metrics"]["outbox"]]
                == initial["source"]["business"]["outbox"]
            )
            if actions:
                assert (
                    final["source"]["business"]["outbox"][-1]["status"]
                    == "review_required"
                )
            before_files = files(original)
            reset = cli(
                "reset",
                "--root",
                original,
                "--new-root",
                root / (case["id"] + "-reset"),
            )
            assert files(original) == before_files
            assert reset["instance_id"] != initial["instance_id"]
            assert reset["seed_sha256"] != initial["seed_sha256"]
            assert reset["generation"] == 2
            assert reset["reset_of"]["source_sha256"] == final["source_sha256"]
            assert reset["source"]["business"] == initial["source"]["business"]
            assert all(
                reset["source"]["records"][k] == []
                for k in ("operations", "observations", "decisions")
            )
            results.append(
                {
                    "id": case["id"],
                    "statuses": statuses,
                    "model_requests": len(requests),
                    "action_requests": len(actions),
                    "prior_metrics": initial["metrics"],
                    "reset_preserved_original": True,
                }
            )
    return {
        "cases": results,
        "count": len(results),
        "real_provider_or_slack": False,
        "execution": "actual pinned SDKs and owned loopback HTTP; explicit synthetic-human updates",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root")
    parser.add_argument("--report")
    args = parser.parse_args()
    if args.root:
        report = qualify(args.root)
    else:
        with tempfile.TemporaryDirectory(prefix="support-installed-cases-") as root:
            report = qualify(root)
    raw = json.dumps(report, indent=2) + "\n"
    if args.report:
        Path(args.report).write_text(raw)
    print(raw)


if __name__ == "__main__":
    main()
