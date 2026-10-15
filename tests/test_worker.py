from pathlib import Path

from fastapi.testclient import TestClient
import pytest

from support_escalation.actions import LocalActions, PipedreamActions
from support_escalation.api import create_app
from support_escalation.model import summarize
from support_escalation.service import Store, Worker


FIXTURE = Path(__file__).parents[1] / "fixtures" / "starter.json"


def seeded(tmp_path):
    return Store.seed(tmp_path / "escalation.db", FIXTURE)


def event(event_id, ticket_id, kind="clock_tick", at="2026-10-15T12:30:00Z"):
    return {"event_id": event_id, "ticket_id": ticket_id, "kind": kind, "occurred_at": at}


def test_due_escalation_is_delivered_once_even_with_repeated_triggers(tmp_path):
    store = seeded(tmp_path)
    calls = []
    def summary(ticket):
        calls.append(ticket["id"])
        return f"Ticket {ticket['id']} has an unanswered customer request."
    worker = Worker(store, summary, LocalActions())
    first = worker.handle(event("EV-501", "T-501"))
    assert first == {"status": "escalated", "ticket_id": "T-501"}
    assert worker.handle(event("EV-501", "T-501")) == first
    assert worker.handle(event("EV-502", "T-501"))["status"] == "not_due_or_resolved"
    assert calls == ["T-501"]
    assert len(store.outbox()) == 1 and store.outbox()[0]["status"] == "delivered"


def test_resolved_replied_and_not_due_tickets_do_not_call_model(tmp_path):
    store = seeded(tmp_path)
    worker = Worker(store, lambda _ticket: (_ for _ in ()).throw(AssertionError("model should not run")), LocalActions())
    assert worker.handle(event("EV-1", "T-502"))["status"] == "not_due_or_resolved"
    assert worker.handle(event("EV-2", "T-503"))["status"] == "not_due_or_resolved"
    assert worker.handle(event("EV-3", "T-504"))["status"] == "not_due_or_resolved"
    assert store.outbox() == []


def test_late_agent_response_during_summary_blocks_escalation(tmp_path):
    store = seeded(tmp_path)
    worker = Worker(store, lambda _ticket: "Ticket T-501 needs attention", LocalActions())
    def late_reply(_ticket):
        worker.handle(event("EV-REPLY", "T-501", "agent_reply", "2026-10-15T12:15:00Z"))
    worker.before_commit = late_reply
    assert worker.handle(event("EV-TICK", "T-501"))["status"] == "changed_before_escalation"
    assert store.outbox() == []
    assert store.ticket("T-501")["last_agent_at"] == "2026-10-15T12:15:00Z"


def test_resolution_during_summary_and_uncertain_notification_are_safe(tmp_path):
    store = seeded(tmp_path)
    worker = Worker(store, lambda _ticket: "Ticket T-501 needs attention", LocalActions())
    worker.before_commit = lambda _ticket: worker.handle(event("EV-RESOLVE", "T-501", "resolved", "2026-10-15T12:20:00Z"))
    assert worker.handle(event("EV-TICK", "T-501"))["status"] == "changed_before_escalation"
    assert store.outbox() == []
    store = seeded(tmp_path / "second")
    class FailingActions:
        def notify(self, *_args):
            raise TimeoutError("remote response lost")
    worker = Worker(store, lambda _ticket: "Ticket T-501 needs attention", FailingActions())
    uncertain = worker.handle(event("EV-TICK", "T-501"))
    assert uncertain["status"] == "review_required"
    assert worker.handle(event("EV-TICK", "T-501")) == uncertain
    assert store.outbox()[0]["status"] == "review_required"
    assert worker.handle(event("EV-TICK-2", "T-501"))["status"] == "not_due_or_resolved"


def test_webhook_token_event_id_conflict_and_clock_seam(tmp_path):
    store = seeded(tmp_path)
    app = create_app(Worker(store, lambda _ticket: "Ticket T-501 needs attention", LocalActions()), "local-secret")
    client = TestClient(app)
    payload = event("EV-1", "T-501", at="2026-10-15T11:00:00Z")
    assert client.post("/events", json=payload).status_code == 401
    assert client.post("/events", json=payload, headers={"x-event-token": "local-secret"}).json()["status"] == "not_due_or_resolved"
    reused = {**payload, "occurred_at": "2026-10-15T12:30:00Z"}
    assert client.post("/events", json=reused, headers={"x-event-token": "local-secret"}).status_code == 409
    assert client.post("/events", json=event("EV-2", "T-501"), headers={"x-event-token": "local-secret"}).json()["status"] == "escalated"


def test_model_failure_before_notification_can_retry_same_event(tmp_path):
    store = seeded(tmp_path)
    calls = 0
    def sometimes_fails(_ticket):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("provider unavailable")
        return "Ticket T-501 needs attention"
    worker = Worker(store, sometimes_fails, LocalActions())
    with pytest.raises(RuntimeError, match="provider unavailable"):
        worker.handle(event("EV-TICK", "T-501"))
    assert store.outbox() == []
    assert worker.handle(event("EV-TICK", "T-501"))["status"] == "escalated"
    assert calls == 2


def test_live_boundaries_require_credentials_and_explicit_pipedream_enablement(tmp_path, monkeypatch):
    store = seeded(tmp_path)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_MODEL", raising=False)
    with pytest.raises(RuntimeError, match="ANTHROPIC_API_KEY"):
        summarize(store.ticket("T-501"))
    monkeypatch.delenv("ALLOW_PIPEDREAM_ACTIONS", raising=False)
    with pytest.raises(RuntimeError, match="Pipedream development configuration"):
        PipedreamActions()
