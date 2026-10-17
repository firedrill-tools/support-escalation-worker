"""Real SQLite scope/read/write/source lifetime and private CLI boundaries."""

import base64
import json
import os
import sqlite3
import subprocess
import sys
from copy import deepcopy
from contextlib import closing
from pathlib import Path

import pytest

from support_escalation.managed import Credential, ManagedSource
from support_escalation.strict import canonical, digest

FIXTURE = Path(__file__).parents[1] / "fixtures/starter.json"


def event(id="EV-CLOCK", kind="clock_tick", at="2026-10-15T12:30:00Z"):
    return {"event_id": id, "ticket_id": "T-501", "kind": kind, "occurred_at": at}


def seeded(tmp_path):
    return ManagedSource.seed(tmp_path / "source.db", FIXTURE)


def begin(source, writes=True, mode="local", id="EV-CLOCK"):
    payload = event(id)
    return source.begin(
        payload,
        source.intent(payload),
        "Evaluate this exact caller ticket event",
        allow_writes=writes,
        mode=mode,
    )


def commit(source, scope, action, read=None):
    read = source.observe(scope) if read is None else read
    return source.commit(scope, action, read["observation"]["id"], read["proof"])


def register(source, scope):
    return commit(source, scope, {"kind": "event", "args": event()})


def reserve(source, scope, read=None, mode="local"):
    read = source.observe(scope) if read is None else read
    return commit(
        source,
        scope,
        {
            "kind": "reserve",
            "args": {
                "event_id": "EV-CLOCK",
                "source": read["value"]["ticket"],
                "summary": "Ticket T-501 needs internal attention.",
                "mode": mode,
            },
        },
        read,
    )


def test_actual_scoped_registration_current_read_atomic_local_queue_and_close(tmp_path):
    with seeded(tmp_path) as source:
        first = source.snapshot()
        scope = begin(source)
        assert register(source, scope)["result"]["status"] == "eligible"
        read = source.observe(scope)
        assert "proof" not in read["value"] and "token" not in canonical(read["value"])
        effect = reserve(source, scope, read)
        assert effect["decision"]["status"] == "applied"
        assert effect["result"]["status"] == "local_queued"
        assert source.close_operation(scope)["closed"]
        assert source.close_operation(scope)["already_closed"]
        final = source.snapshot()
        assert final["source"] == first["source"]
        assert (
            final["business"]["outbox"][0]["source"] == first["business"]["tickets"][0]
        )
        assert final["records"]["operations"][0]["status"] == "closed"
        assert len(final["records"]["decisions"]) == 2


def test_live_credential_pins_complete_original_permission_and_caller_authority(
    tmp_path,
):
    with seeded(tmp_path) as source:
        scope = begin(source, writes=False)
        operation = json.loads(
            source.database.execute("SELECT document FROM operations").fetchone()[0]
        )
        operation["allow_writes"] = True
        source.database.execute(
            "UPDATE operations SET document=?", (canonical(operation),)
        )
        # This is otherwise a self-consistent empty source. The live credential
        # still owns its original readonly caller authority.
        assert source.snapshot()["business"]["events"] == []
        with pytest.raises(ValueError, match="own active"):
            source.observe(scope)
        assert source.close_operation(scope)["closed"]
        assert source.snapshot()["records"]["observations"] == []


@pytest.mark.parametrize(
    "fault",
    [
        "guessed_token",
        "guessed_proof",
        "wrong_id",
        "consumed",
        "closed",
        "foreign_source",
        "stale",
    ],
)
def test_no_guessed_borrowed_consumed_closed_foreign_or_stale_read_authority(
    tmp_path, fault
):
    with seeded(tmp_path) as source:
        scope = begin(source)
        read = source.observe(scope)
        selected = scope
        proof = read["proof"]
        id = read["observation"]["id"]
        if fault == "guessed_token":
            selected = Credential(scope.id, "guessed")
        elif fault == "guessed_proof":
            proof = "guessed"
        elif fault == "wrong_id":
            id += 1
        elif fault == "consumed":
            register(source, scope)
        elif fault == "closed":
            source.close_operation(scope)
        elif fault == "foreign_source":
            other = ManagedSource.seed(tmp_path / "other.db", FIXTURE)
            try:
                other_scope = begin(other)
                other_read = other.observe(other_scope)
                proof = other_read["proof"]
            finally:
                other.close()
        else:
            source.event(event("EV-CHANGE", "agent_reply", "2026-10-15T12:00:00Z"))
        before = source.snapshot()["business"]
        with pytest.raises(ValueError):
            source.commit(selected, {"kind": "event", "args": event()}, id, proof)
        assert source.snapshot()["business"] == before
        if fault != "closed":
            source.close_operation(scope)


def test_separate_write_gate_denies_without_rewriting_literal_business_cell(tmp_path):
    with seeded(tmp_path) as source:
        scope = begin(source, writes=False)
        original = source.database.execute(
            "SELECT CAST(document AS BLOB) FROM business"
        ).fetchone()[0]
        decision = register(source, scope)
        assert decision["decision"]["status"] == "denied"
        assert decision["result"]["status"] == "write_permission_required"
        assert (
            source.database.execute(
                "SELECT CAST(document AS BLOB) FROM business"
            ).fetchone()[0]
            == original
        )
        assert source.snapshot()["business"]["events"] == []
        source.close_operation(scope)


def test_current_independent_reply_invalidates_old_observation_and_draft(tmp_path):
    with seeded(tmp_path) as source:
        scope = begin(source)
        register(source, scope)
        read = source.observe(scope)
        with ManagedSource(source.path) as other:
            other.event(event("EV-LATE", "agent_reply", "2026-10-15T12:15:00Z"))
        with pytest.raises(ValueError, match="current unused"):
            reserve(source, scope, read)
        result = reserve(source, scope)
        assert result["result"]["status"] == "changed_before_escalation"
        assert source.snapshot()["business"]["outbox"] == []
        source.close_operation(scope)


@pytest.mark.parametrize(
    "fault",
    [
        "intent_ticket",
        "intent_event",
        "intent_context",
        "caller_event",
        "draft_ticket",
        "mode",
    ],
)
def test_complete_caller_event_ticket_and_action_identity(tmp_path, fault):
    with seeded(tmp_path) as source:
        payload = event()
        intent = source.intent(payload)
        if fault.startswith("intent"):
            if fault == "intent_ticket":
                intent["ticket"]["customer"] = "Other customer"
            elif fault == "intent_event":
                intent["event"]["ticket_id"] = "T-504"
            else:
                intent["context_sha256"] = "a" * 64
            with pytest.raises(ValueError, match="intent"):
                source.begin(payload, intent, "Exact caller", allow_writes=True)
            assert source.snapshot()["records"]["operations"] == []
            return
        scope = begin(source)
        register(source, scope)
        read = source.observe(scope)
        if fault == "caller_event":
            action = {"kind": "event", "args": event("EV-OTHER")}
        else:
            row = deepcopy(read["value"]["ticket"])
            if fault == "draft_ticket":
                row["customer"] = "Other customer"
            action = {
                "kind": "reserve",
                "args": {
                    "event_id": "EV-CLOCK",
                    "source": row,
                    "summary": "Exact selected ticket",
                    "mode": "pipedream" if fault == "mode" else "local",
                },
            }
        before = source.snapshot()["business"]
        with pytest.raises(ValueError):
            commit(source, scope, action, read)
        assert source.snapshot()["business"] == before
        source.close_operation(scope)


def test_one_active_scope_and_real_sqlite_writer_lock_reject_without_registration(
    tmp_path,
):
    with seeded(tmp_path) as source:
        scope = begin(source)
        with ManagedSource(source.path) as other:
            with pytest.raises(ValueError, match="active operation"):
                begin(other, id="EV-OTHER")
        source.close_operation(scope)
        with closing(sqlite3.connect(source.path, isolation_level=None)) as lock:
            lock.execute("BEGIN IMMEDIATE")
            try:
                with pytest.raises(sqlite3.OperationalError, match="locked"):
                    begin(source, id="EV-OTHER")
            finally:
                lock.execute("ROLLBACK")
        assert len(source.snapshot()["records"]["operations"]) == 1


def test_cancelled_precommit_rolls_back_but_cannot_block_control_revocation(tmp_path):
    with seeded(tmp_path) as source:
        scope = begin(source)
        read = source.observe(scope)
        before = source.snapshot()

        def fail():
            raise RuntimeError("caller cancelled")

        source.commit_guard = fail
        with pytest.raises(RuntimeError, match="cancelled"):
            commit(source, scope, {"kind": "event", "args": event()}, read)
        assert source.snapshot() == before
        assert source.close_operation(scope)["closed"]


@pytest.mark.parametrize(
    "fault", ["business", "decision", "read", "invalid_utf8", "schema", "header"]
)
def test_complete_original_sql_and_replay_refuse_damage_without_repair(tmp_path, fault):
    with seeded(tmp_path) as source:
        scope = begin(source)
        register(source, scope)
        reserve(source, scope)
        source.close_operation(scope)
        db = source.database
        if fault == "business":
            value = json.loads(
                db.execute("SELECT document FROM business").fetchone()[0]
            )
            value["tickets"][0]["customer"] = "Changed"
            db.execute("UPDATE business SET document=?", (canonical(value),))
        elif fault in ("decision", "read"):
            table = "decisions" if fault == "decision" else "observations"
            value = json.loads(
                db.execute("SELECT document FROM " + table + " WHERE id=1").fetchone()[
                    0
                ]
            )
            if fault == "decision":
                value["result"] = {"status": "fabricated"}
            else:
                value["value"]["ticket"]["customer"] = "Fabricated"
            db.execute(
                "UPDATE " + table + " SET document=? WHERE id=1", (canonical(value),)
            )
        elif fault == "invalid_utf8":
            db.execute(
                "UPDATE business SET document=CAST(x'7b226b223a2280227d' AS TEXT)"
            )
        elif fault == "schema":
            db.execute("CREATE TABLE unexpected(value TEXT)")
        else:
            value = json.loads(
                db.execute("SELECT document FROM metadata").fetchone()[0]
            )
            raw = base64.b64decode(value["fixture_base64"]) + b" "
            from support_escalation.strict import sha256

            value["fixture_base64"] = base64.b64encode(raw).decode()
            value["fixture_sha256"] = sha256(raw)
            db.execute("UPDATE metadata SET document=?", (canonical(value),))
        before = source.path.read_bytes()
        with pytest.raises((ValueError, UnicodeError)):
            source.snapshot()
        assert source.path.read_bytes() == before


@pytest.mark.parametrize("fault", ["business", "decision", "observation"])
def test_control_only_revocation_survives_damaged_business_history(tmp_path, fault):
    with seeded(tmp_path) as source:
        scope = begin(source)
        register(source, scope)
        table = {
            "business": "business",
            "decision": "decisions",
            "observation": "observations",
        }[fault]
        source.database.execute("UPDATE " + table + " SET document='{}' WHERE id=1")
        assert source.close_operation(scope)["closed"]
        value = json.loads(
            source.database.execute("SELECT document FROM operations").fetchone()[0]
        )
        assert value["status"] == "closed"
        with pytest.raises(ValueError):
            source.snapshot()


def test_closed_scope_cannot_restart_same_already_recorded_event(tmp_path):
    with seeded(tmp_path) as source:
        scope = begin(source)
        register(source, scope)
        source.close_operation(scope)
        with pytest.raises(ValueError, match="already_recorded"):
            begin(source)
        assert len(source.snapshot()["records"]["operations"]) == 1


def test_complete_replay_refuses_forged_later_decision_under_a_closed_scope(tmp_path):
    from support_escalation.domain import transition
    from support_escalation.managed_state import context

    with seeded(tmp_path) as source:
        scope = begin(source)
        register(source, scope)
        read = source.observe(scope)
        source.close_operation(scope)
        original = source.snapshot()
        action = {
            "kind": "reserve",
            "args": {
                "event_id": "EV-CLOCK",
                "source": read["value"]["ticket"],
                "summary": "Forged post-close draft",
                "mode": "local",
            },
        }
        after, result = transition(original["business"], action)
        _, after_context = context(original["source"], 2, after)
        forged = {
            "id": 2,
            "action": action,
            "operation_id": scope.id,
            "read_id": read["observation"]["id"],
            "before_context_sha256": original["context_sha256"],
            "after_context_sha256": after_context,
            "status": "applied",
            "result": result,
            "provenance": "managed_caller",
        }
        source.database.execute("UPDATE business SET document=?", (canonical(after),))
        source.database.execute(
            "INSERT INTO decisions VALUES (2,?)", (canonical(forged),)
        )
        before = source.path.read_bytes()
        with pytest.raises(ValueError, match="own current unused"):
            source.snapshot()
        assert source.path.read_bytes() == before


def test_missing_aliased_and_replaced_managed_source_never_creates_or_mutates(tmp_path):
    missing = tmp_path / "missing.db"
    with pytest.raises(FileNotFoundError):
        ManagedSource(missing)
    assert not missing.exists()
    with seeded(tmp_path) as source:
        alias = tmp_path / "alias.db"
        alias.symlink_to(source.path)
        with pytest.raises(ValueError):
            ManagedSource(alias)
        backup = tmp_path / "backup.db"
        backup.write_bytes(source.path.read_bytes())
        source.path.unlink()
        source.path.write_bytes(backup.read_bytes())
        before = source.path.read_bytes()
        with pytest.raises(ValueError, match="identity"):
            source.snapshot()
        assert source.path.read_bytes() == before
        with pytest.raises(ValueError, match="identity"):
            ManagedSource(backup)


def test_actual_owned_database_and_retained_descriptor_close_explicitly_once(tmp_path):
    source = seeded(tmp_path)
    fd = source.descriptor
    db = source.database
    assert source.path.stat().st_mode & 0o777 == 0o600
    result = source.close()
    assert result["database_closed"] and result["descriptor_closed"]
    assert source.close() == result
    with pytest.raises(OSError):
        os.fstat(fd)
    with pytest.raises(sqlite3.ProgrammingError):
        db.execute("SELECT 1")


def test_private_actual_cli_has_no_model_capabilities_and_closes_before_success(
    tmp_path,
):
    database = tmp_path / "cli.db"

    def cli(*args, status=0):
        p = subprocess.run(
            [sys.executable, "-m", "support_escalation.managed_cli", *args],
            capture_output=True,
            text=True,
            timeout=15,
        )
        assert p.returncode == status, p.stdout + p.stderr
        return json.loads(p.stdout)

    seeded = cli("init", "--db", str(database), "--fixture", str(FIXTURE))
    assert seeded == cli("inspect", "--db", str(database))
    assert all(
        not seeded["records"][name]
        for name in ("operations", "observations", "decisions")
    )
    assert (
        cli("event", "--db", str(database), "--event-json", json.dumps(event()))[
            "result"
        ]["status"]
        == "eligible"
    )
    cli("init", "--db", str(database), "--fixture", str(FIXTURE), status=2)
