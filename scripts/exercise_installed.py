"""Production-only installed entry points, SDK HTTP, retained effects and all signals."""

import importlib.util
import json
import os
import signal
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import support_escalation

BASE = Path(sys.argv[1])
BASE.mkdir()
assert (
    Path(support_escalation.__file__)
    .resolve()
    .is_relative_to(Path(sys.prefix).resolve())
)
if "--production-only" in sys.argv:
    assert importlib.util.find_spec("pytest") is None
BIN = Path(sys.executable).parent
ENV = {
    k: os.environ[k]
    for k in ("PATH", "HOME", "TMPDIR", "TMP", "TEMP", "LANG", "SystemRoot", "WINDIR")
    if k in os.environ
}
KEY, MODEL = "synthetic-installed-support-key", "claude-owned-installation"
EVENT = {
    "event_id": "EV-RUN-0001",
    "ticket_id": "T-501",
    "kind": "clock_tick",
    "occurred_at": "2026-10-15T12:30:00Z",
}


def execute(command, expected=0, env=ENV):
    result = subprocess.run(
        list(map(str, command)), cwd=BASE, env=env, capture_output=True, timeout=45
    )
    assert result.returncode == expected, (result.stdout[-5000:], result.stderr[-5000:])
    return json.loads(result.stdout)


qualifier = Path(__file__).with_name("qualify_cases.py")
probe = execute(
    [sys.executable, "-I", qualifier, "--root", BASE / "populations"], env=ENV
)
assert probe["count"] == 12
launcher = BASE / "support-escalation-local"
launcher.symlink_to(BIN / "support-escalation-local")
proof = []
for mode in ("completed", "unfinished-after-clock", "SIGINT", "SIGTERM", "SIGHUP"):
    case = execute(
        [
            BIN / "support-escalation-cases",
            "init",
            "--case",
            "starter-due",
            "--root",
            BASE / mode,
        ]
    )
    entered, release = threading.Event(), threading.Event()
    requests, errors = [], []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            try:
                body = json.loads(self.rfile.read(int(self.headers["content-length"])))
                assert self.path == "/v1/messages"
                assert self.headers.get("x-api-key") == KEY
                assert self.headers.get("anthropic-version") == "2023-06-01"
                requests.append(body)
                public = json.loads(body["messages"][0]["content"])
                row = public["ticket"]
                assert row["id"] == "T-501"
                if mode.startswith("SIG"):
                    entered.set()
                    assert release.wait(30)
                value = {
                    "id": "msg_installed_1",
                    "type": "message",
                    "role": "assistant",
                    "model": MODEL,
                    "content": [
                        {
                            "type": "text",
                            "text": f"Ticket {row['id']}: {row['subject']}. Customer unanswered since {row['last_customer_at']}.",
                        }
                    ],
                    "stop_reason": (
                        "max_tokens" if mode == "unfinished-after-clock" else "end_turn"
                    ),
                    "stop_sequence": None,
                    "usage": {"input_tokens": 10, "output_tokens": 2},
                    "original_unknown": {"retained": True},
                }
                raw = json.dumps(value).encode()
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
            except (BrokenPipeError, ConnectionResetError):
                pass
            except BaseException as error:
                errors.append(str(error))

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    command = [
        launcher,
        "--db",
        case["database"],
        "--event-json",
        json.dumps(EVENT),
        "--allow-writes",
        "--model",
        MODEL,
        "--base-url",
        f"http://127.0.0.1:{server.server_port}",
        "--output",
        BASE / mode / "receipts",
    ]
    child = subprocess.Popen(
        list(map(str, command)),
        cwd=BASE,
        env={**ENV, "ANTHROPIC_API_KEY": KEY},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        if mode.startswith("SIG"):
            assert entered.wait(15)
            child.send_signal(getattr(signal, mode))
        stdout, stderr = child.communicate(timeout=30)
        assert child.returncode == (0 if mode == "completed" else 1), (
            stdout[-5000:],
            stderr[-5000:],
        )
        result = json.loads(stdout)
        receipt_path = Path(result["receipt_path"])
        receipt = json.loads(receipt_path.read_text())
        assert receipt["cleanup_confirmed"]
        assert len(requests) == 1
        assert len(receipt["acknowledged_effects"]) == (2 if mode == "completed" else 1)
        final = execute(
            [BIN / "support-escalation-cases", "inspect", "--root", case["root"]]
        )
        assert all(
            o["status"] == "closed" for o in final["source"]["records"]["operations"]
        )
        assert len(final["source"]["business"]["events"]) == 1
        assert len(final["source"]["business"]["outbox"]) == (
            1 if mode == "completed" else 0
        )
        assert KEY not in json.dumps(receipt)
        wire = json.dumps(requests)
        for private in (
            case["case_id"],
            case["instance_id"],
            case["seed_sha256"],
            case["root"],
            "fixture_base64",
            "definition_sha256",
        ):
            assert private not in wire
        if mode == "completed":
            assert (
                result["status"] == "local_queued"
                and result["business"]["local_queue_complete"]
            )
            assert result["cost_usd"] is None
        else:
            assert receipt["status"] == "failed"
            assert not receipt["observed_business"]["workflow_complete"]
        source_bytes = Path(case["database"]).read_bytes()
        evidence_bytes = receipt_path.read_bytes()
        fresh = execute(
            [
                BIN / "support-escalation-cases",
                "reset",
                "--root",
                case["root"],
                "--new-root",
                BASE / (mode + "-reset"),
            ]
        )
        assert fresh["instance_id"] != case["instance_id"] and fresh["generation"] == 2
        assert fresh["source"]["business"] == case["source"]["business"]
        assert (
            Path(case["database"]).read_bytes() == source_bytes
            and receipt_path.read_bytes() == evidence_bytes
        )
        proof.append(
            {
                "mode": mode,
                "requests": 1,
                "acknowledged_effects": len(receipt["acknowledged_effects"]),
                "cleanup_confirmed": True,
                "preserving_reset": True,
            }
        )
    finally:
        release.set()
        if child.poll() is None:
            child.kill()
            child.communicate(timeout=5)
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive() and errors == [], errors

seed = execute([BIN / "support-escalation", "--seed", "--db", BASE / "default.db"])
assert seed["initialized"] and seed["source_version"] == 2
default = execute(
    [BIN / "support-escalation-state", "inspect", "--db", BASE / "default.db"]
)
assert len(default["business"]["tickets"]) == 4
missing = execute(
    [
        launcher,
        "--db",
        BASE / "absent.db",
        "--event-json",
        json.dumps(EVENT),
        "--model",
        MODEL,
        "--output",
        BASE / "missing-key",
    ],
    expected=1,
)
assert not (BASE / "absent.db").exists()
missing_receipt = json.loads(Path(missing["receipt_path"]).read_text())
assert missing_receipt["allocation"] == {
    "source": False,
    "scope": False,
    "provider": False,
}
print(
    json.dumps(
        {
            "cases": probe["count"],
            "normal_sdk_cli": proof,
            "default_seed": True,
            "missing_key_before_source": True,
            "production_only": "--production-only" in sys.argv,
            "real_provider_or_slack": False,
        }
    )
)
