"""Actual original bytes, event-driven source history and retained file owners."""

import json
import os
from copy import deepcopy
from pathlib import Path

import pytest

from support_escalation.domain import eligible, initial, transition
from support_escalation.source import parse
from support_escalation.strict import MAX_INTEGER, digest, parse_json
from support_escalation.workspace import Workspace

FIXTURE = Path(__file__).parents[1] / "fixtures/starter.json"


def event(id="EV-CLOCK", kind="clock_tick", at="2026-10-15T12:30:00Z"):
    return {
        "kind": "event",
        "args": {"event_id": id, "ticket_id": "T-501", "kind": kind, "occurred_at": at},
    }


def source(tmp_path, raw=None):
    file = tmp_path / "source.json"
    file.write_bytes(FIXTURE.read_bytes() if raw is None else raw)
    return Workspace(file)


def reserve(workspace, mode="local"):
    row = workspace.inspect()["business"]["tickets"][0]
    return workspace.apply(
        {
            "kind": "reserve",
            "args": {
                "event_id": "EV-CLOCK",
                "source": row,
                "summary": "Ticket T-501 requires internal attention.",
                "mode": mode,
            },
        }
    )


def test_original_starter_and_exact_two_hour_clock(tmp_path):
    with source(tmp_path) as workspace:
        state = workspace.inspect()
        assert state["origin"] is None and len(state["business"]["tickets"]) == 4
        row = state["business"]["tickets"][0]
        assert not eligible(row, "2026-10-15T11:59:59Z")
        assert eligible(row, "2026-10-15T12:00:00Z")
        assert not eligible(row, "2026-10-15T09:59:59Z")
        assert workspace.path.read_bytes() == FIXTURE.read_bytes()


def test_local_queue_has_current_full_snapshot_and_no_remote_delivery_claim(tmp_path):
    with source(tmp_path) as workspace:
        assert workspace.apply(event())["status"] == "eligible"
        assert reserve(workspace) == {"status": "local_queued", "ticket_id": "T-501"}
        state = workspace.inspect()
        outbox = state["business"]["outbox"][0]
        assert outbox["source_sha256"] == digest(outbox["source"])
        assert outbox["acknowledgement"] is None
        assert state["business"]["tickets"][0]["revision"] == 2
        assert len(state["history"]) == 2
        before = workspace.path.read_bytes()
        assert workspace.apply(event())["status"] == "local_queued"
        assert reserve(workspace)["status"] == "local_queued"
        assert workspace.path.read_bytes() == before


@pytest.mark.parametrize("kind", ["agent_reply", "resolved", "customer_reply"])
def test_full_current_snapshot_blocks_late_changes(tmp_path, kind):
    with source(tmp_path) as workspace:
        workspace.apply(event())
        original = deepcopy(workspace.inspect()["business"]["tickets"][0])
        workspace.apply(event("EV-CHANGE", kind, "2026-10-15T12:15:00Z"))
        result = workspace.apply(
            {
                "kind": "reserve",
                "args": {
                    "event_id": "EV-CLOCK",
                    "source": original,
                    "summary": "Earlier draft",
                    "mode": "local",
                },
            }
        )
        assert result["status"] == "changed_before_escalation"
        assert workspace.inspect()["business"]["outbox"] == []


def test_repeated_conflicting_stale_and_resolved_events(tmp_path):
    with source(tmp_path) as workspace:
        result = workspace.apply(
            event("EV-REPLY", "agent_reply", "2026-10-15T11:00:00Z")
        )
        before = workspace.path.read_bytes()
        assert (
            workspace.apply(event("EV-REPLY", "agent_reply", "2026-10-15T11:00:00Z"))
            == result
        )
        assert workspace.path.read_bytes() == before
        with pytest.raises(ValueError, match="reused"):
            workspace.apply(event("EV-REPLY", "agent_reply", "2026-10-15T11:01:00Z"))
        assert workspace.path.read_bytes() == before
        assert (
            workspace.apply(event("EV-STALE", "agent_reply", "2026-10-15T10:30:00Z"))[
                "status"
            ]
            == "stale_event"
        )
        assert (
            workspace.apply(
                event("EV-OLD-RESOLVE", "resolved", "2026-10-15T09:00:00Z")
            )["status"]
            == "stale_event"
        )
        assert (
            workspace.apply(event("EV-RESOLVE", "resolved", "2026-10-15T11:01:00Z"))[
                "status"
            ]
            == "recorded"
        )
        assert (
            workspace.apply(event("EV-LATE", "customer_reply", "2026-10-15T12:00:00Z"))[
                "status"
            ]
            == "already_resolved"
        )


def test_uncertain_remote_reservation_never_replays_or_claims_delivery(tmp_path):
    with source(tmp_path) as workspace:
        workspace.apply(event())
        assert reserve(workspace, "pipedream")["status"] == "notification_started"
        result = workspace.apply(
            {
                "kind": "notification_observation",
                "args": {
                    "event_id": "EV-CLOCK",
                    "status": "review_required",
                    "response_sha256": None,
                },
            }
        )
        assert result["status"] == "review_required"
        before = workspace.path.read_bytes()
        assert workspace.apply(event())["status"] == "review_required"
        assert reserve(workspace, "pipedream")["status"] == "review_required"
        assert workspace.path.read_bytes() == before
        assert workspace.apply(event("EV-OTHER"))["status"] == "not_due_or_resolved"
        with pytest.raises(ValueError, match="never replay"):
            workspace.apply(
                {
                    "kind": "notification_observation",
                    "args": {
                        "event_id": "EV-CLOCK",
                        "status": "remote_acknowledged",
                        "response_sha256": "a" * 64,
                    },
                }
            )
        assert (
            workspace.inspect()["business"]["outbox"][0]["acknowledgement"][
                "actual_delivery"
            ]
            is None
        )


def test_final_calendar_boundary_uses_subtraction_without_overflow():
    state = initial(json.loads(FIXTURE.read_bytes()))
    row = state["tickets"][0]
    row["last_customer_at"] = "9999-12-31T21:59:59Z"
    assert eligible(row, "9999-12-31T23:59:59Z")
    row["last_customer_at"] = "9999-12-31T23:59:59Z"
    assert not eligible(row, "9999-12-31T23:59:59Z")


def test_safe_revision_boundary_before_any_source_effect():
    state = initial(json.loads(FIXTURE.read_bytes()))
    state["tickets"][0]["revision"] = MAX_INTEGER - 1
    after, result = transition(
        state, event("EV-LAST", "agent_reply", "2026-10-15T11:00:00Z")
    )
    assert result["revision"] == MAX_INTEGER
    before = deepcopy(after)
    with pytest.raises(ValueError, match="revision_exhausted"):
        transition(
            after, event("EV-OVERFLOW", "customer_reply", "2026-10-15T12:00:00Z")
        )
    assert after == before


@pytest.mark.parametrize(
    "raw",
    [
        b'{"tickets":[],"tickets":[]}',
        b'{"tickets":NaN}',
        b'{"tickets":1e400}',
        b'{"tickets":-0}',
        b'{"tickets":-0.0}',
        b'{"tickets":"\\ud800"}',
        b'{"tickets":"\\u0000"}',
        b"\xff",
        b"[" * 66 + b"0" + b"]" * 66,
        b'{"tickets":[]} trailing',
    ],
)
def test_original_json_ambiguity_refuses(raw):
    with pytest.raises((ValueError, UnicodeError)):
        parse_json(raw)


@pytest.mark.parametrize(
    "mutation", ["bool_status", "bool_time", "offset_time", "extra", "duplicate_id"]
)
def test_complete_original_fixture_refuses_without_repair(tmp_path, mutation):
    value = json.loads(FIXTURE.read_bytes())
    if mutation == "bool_status":
        value["tickets"][0]["status"] = True
    elif mutation == "bool_time":
        value["tickets"][0]["last_customer_at"] = True
    elif mutation == "offset_time":
        value["tickets"][0]["last_customer_at"] = "2026-10-15T10:00:00+00:00"
    elif mutation == "extra":
        value["tickets"][0]["revision"] = 1
    else:
        value["tickets"].append(deepcopy(value["tickets"][0]))
    raw = json.dumps(value).encode()
    file = tmp_path / "source.json"
    file.write_bytes(raw)
    with pytest.raises(ValueError):
        Workspace(file)
    assert file.read_bytes() == raw


@pytest.mark.parametrize(
    "mutation", ["draft", "customer", "history", "rehash", "original", "event_result"]
)
def test_complete_history_rejects_fabricated_or_rehashed_source(tmp_path, mutation):
    with source(tmp_path) as workspace:
        workspace.apply(event())
        reserve(workspace)
        state = json.loads(workspace.path.read_bytes())
    if mutation == "draft":
        state["business"]["outbox"][0]["summary"] = "Fabricated draft"
    elif mutation == "customer":
        state["business"]["tickets"][0]["customer"] = "Another identity"
    elif mutation == "history":
        state["history"].pop()
    elif mutation == "rehash":
        state["history"][-1]["result"] = {"status": "escalated"}
        state["history"][-1]["after_sha256"] = digest(state["business"])
    elif mutation == "original":
        state["origin"]["fixture_sha256"] = "a" * 64
    else:
        state["business"]["events"][0]["result"] = {"status": "delivered"}
    with pytest.raises(ValueError):
        parse(json.dumps(state).encode())


@pytest.mark.parametrize("alias", ["symlink", "hardlink"])
def test_source_alias_refuses_without_touching_target(tmp_path, alias):
    target = tmp_path / "target.json"
    target.write_bytes(FIXTURE.read_bytes())
    other = tmp_path / "other.json"
    if alias == "symlink":
        other.symlink_to(target)
    else:
        os.link(target, other)
    before = target.read_bytes()
    with pytest.raises((ValueError, OSError)):
        Workspace(other)
    assert target.read_bytes() == before


def test_retained_original_guard_rejects_replacement_and_missing_without_creation(
    tmp_path,
):
    with source(tmp_path) as workspace:
        workspace.path.unlink()
        workspace.path.write_bytes(FIXTURE.read_bytes())
        before = workspace.path.read_bytes()
        with pytest.raises(ValueError, match="identity"):
            workspace.apply(event())
        assert workspace.path.read_bytes() == before
    with Workspace(workspace.path) as current:
        current.path.unlink()
        with pytest.raises(FileNotFoundError):
            current.inspect()
        assert not current.path.exists()
