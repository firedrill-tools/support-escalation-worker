"""Durable synthetic ticket events and rechecked escalation delivery."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable


def instant(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("event time must include timezone")
    return parsed.astimezone(timezone.utc)


def eligible(ticket: dict, now: str) -> bool:
    if ticket["status"] != "open" or ticket["escalated_at"]:
        return False
    customer = instant(ticket["last_customer_at"])
    agent = instant(ticket["last_agent_at"]) if ticket["last_agent_at"] else None
    return (not agent or agent < customer) and instant(now) >= customer + timedelta(hours=2)


class Store:
    def __init__(self, path: str | Path):
        self.path = Path(path).resolve()
        if not self.path.is_file():
            raise ValueError("escalation database missing; seed it first")

    @classmethod
    def seed(cls, path: str | Path, fixture: str | Path) -> "Store":
        db = Path(path).resolve()
        if db.exists():
            raise ValueError("seed destination already exists")
        db.parent.mkdir(parents=True, exist_ok=True)
        data = json.loads(Path(fixture).read_text())
        with sqlite3.connect(db) as conn:
            conn.executescript("""
                CREATE TABLE tickets (id TEXT PRIMARY KEY, customer TEXT NOT NULL, subject TEXT NOT NULL,
                  status TEXT NOT NULL, last_customer_at TEXT NOT NULL, last_agent_at TEXT,
                  escalated_at TEXT, revision INTEGER NOT NULL);
                CREATE TABLE events (id TEXT PRIMARY KEY, payload TEXT NOT NULL, status TEXT NOT NULL,
                  result TEXT);
                CREATE TABLE outbox (ticket_id TEXT PRIMARY KEY, event_id TEXT NOT NULL, text TEXT NOT NULL,
                  status TEXT NOT NULL);
            """)
            for row in data["tickets"]:
                conn.execute("INSERT INTO tickets VALUES (?,?,?,?,?,?,NULL,1)",
                             (row["id"], row["customer"], row["subject"], row["status"],
                              row["last_customer_at"], row["last_agent_at"]))
        return cls(db)

    def connect(self):
        conn = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    def ticket(self, ticket_id: str) -> dict:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM tickets WHERE id=?", (ticket_id,)).fetchone()
            if row is None:
                raise ValueError("unknown ticket")
            return dict(row)

    def outbox(self) -> list[dict]:
        with self.connect() as conn:
            return [dict(row) for row in conn.execute("SELECT * FROM outbox ORDER BY ticket_id")]

    def _begin(self, conn):
        conn.execute("BEGIN IMMEDIATE")

    def start(self, event: dict) -> dict | None:
        payload = json.dumps(event, sort_keys=True)
        with self.connect() as conn:
            self._begin(conn)
            old = conn.execute("SELECT payload,status,result FROM events WHERE id=?", (event["event_id"],)).fetchone()
            if old:
                if old["payload"] != payload:
                    raise ValueError("event ID reused with different payload")
                if old["status"] == "started":
                    return {"status": "review_required", "reason": "prior attempt outcome uncertain"}
                return json.loads(old["result"])
            if not conn.execute("SELECT 1 FROM tickets WHERE id=?", (event["ticket_id"],)).fetchone():
                raise ValueError("unknown ticket")
            conn.execute("INSERT INTO events VALUES (?,?,?,NULL)", (event["event_id"], payload, "started"))
            conn.commit()
        return None

    def finish(self, event_id: str, result: dict) -> dict:
        with self.connect() as conn:
            conn.execute("UPDATE events SET status='complete',result=? WHERE id=?", (json.dumps(result), event_id))
        return result

    def abandon_before_notification(self, event_id: str) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM events WHERE id=? AND status='started' AND NOT EXISTS "
                         "(SELECT 1 FROM outbox WHERE event_id=?)", (event_id, event_id))

    def update_ticket(self, event: dict) -> dict:
        with self.connect() as conn:
            self._begin(conn)
            row = dict(conn.execute("SELECT * FROM tickets WHERE id=?", (event["ticket_id"],)).fetchone())
            kind = event["kind"]
            at = event["occurred_at"]
            if kind == "resolved":
                if row["status"] == "resolved":
                    return {"status": "already_resolved"}
                row["status"] = "resolved"
            elif row["status"] == "resolved":
                return {"status": "already_resolved"}
            elif kind == "agent_reply" and (not row["last_agent_at"] or instant(at) > instant(row["last_agent_at"])):
                row["last_agent_at"] = at
            elif kind == "customer_reply" and instant(at) > instant(row["last_customer_at"]):
                row["last_customer_at"] = at
            else:
                return {"status": "stale_event"}
            conn.execute("UPDATE tickets SET status=?,last_customer_at=?,last_agent_at=?,revision=revision+1 WHERE id=?",
                         (row["status"], row["last_customer_at"], row["last_agent_at"], row["id"]))
            conn.commit()
            return {"status": "recorded", "revision": row["revision"] + 1}

    def reserve_escalation(self, event: dict, summary: str, expected_revision: int) -> dict:
        with self.connect() as conn:
            self._begin(conn)
            row = dict(conn.execute("SELECT * FROM tickets WHERE id=?", (event["ticket_id"],)).fetchone())
            if row["revision"] != expected_revision or not eligible(row, event["occurred_at"]):
                return {"status": "changed_before_escalation"}
            conn.execute("UPDATE tickets SET escalated_at=?,revision=revision+1 WHERE id=?", (event["occurred_at"], row["id"]))
            conn.execute("INSERT INTO outbox VALUES (?,?,?,?)", (row["id"], event["event_id"], summary, "notification_started"))
            conn.commit()
            return {"status": "notification_started", "ticket_id": row["id"]}

    def finish_notification(self, ticket_id: str, delivered: bool) -> None:
        with self.connect() as conn:
            conn.execute("UPDATE outbox SET status=? WHERE ticket_id=?", ("delivered" if delivered else "review_required", ticket_id))


class Worker:
    def __init__(self, store: Store, summarize: Callable[[dict], str], actions, before_commit=None):
        self.store = store
        self.summarize = summarize
        self.actions = actions
        self.before_commit = before_commit

    def handle(self, event: dict) -> dict:
        if event["kind"] not in {"clock_tick", "agent_reply", "customer_reply", "resolved"}:
            raise ValueError("invalid event kind")
        instant(event["occurred_at"])
        prior = self.store.start(event)
        if prior is not None:
            return prior
        if event["kind"] != "clock_tick":
            return self.store.finish(event["event_id"], self.store.update_ticket(event))
        ticket = self.store.ticket(event["ticket_id"])
        if not eligible(ticket, event["occurred_at"]):
            return self.store.finish(event["event_id"], {"status": "not_due_or_resolved"})
        try:
            summary = self.summarize(ticket)
            if not summary.strip():
                raise ValueError("empty escalation summary")
        except Exception:
            self.store.abandon_before_notification(event["event_id"])
            raise
        if self.before_commit:
            self.before_commit(ticket)
        reservation = self.store.reserve_escalation(event, summary, ticket["revision"])
        if reservation["status"] != "notification_started":
            return self.store.finish(event["event_id"], reservation)
        try:
            self.actions.notify(event["event_id"], ticket["id"], summary)
        except Exception:
            self.store.finish_notification(ticket["id"], False)
            return self.store.finish(event["event_id"], {"status": "review_required", "reason": "notification outcome uncertain"})
        self.store.finish_notification(ticket["id"], True)
        return self.store.finish(event["event_id"], {"status": "escalated", "ticket_id": ticket["id"]})
