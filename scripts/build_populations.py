"""Deterministically author fictional histories through the real human Workspace."""

import argparse
import base64
import json
import tempfile
from copy import deepcopy
from pathlib import Path

from support_escalation.source import PROVENANCE, parse
from support_escalation.strict import MAX_INTEGER, sha256
from support_escalation.workspace import Workspace

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "src/support_escalation/data"


def encoded(value):
    return (
        json.dumps(value, ensure_ascii=False, allow_nan=False, indent=2) + "\n"
    ).encode()


def event(ticket, n, kind="clock_tick", at="2026-10-15T13:00:00Z"):
    return {"event_id": f"EV-{n}", "ticket_id": ticket, "kind": kind, "occurred_at": at}


def author(raw, actions):
    with tempfile.TemporaryDirectory(prefix="support-author-") as directory:
        path = Path(directory) / "fixture.json"
        path.write_bytes(raw)
        with Workspace(path) as owner:
            for action in actions:
                if callable(action):
                    action = action(owner.inspect()["business"])
                owner.apply(action)
        return path.read_bytes()


def reserve(payload, mode):
    def action(business):
        row = next(t for t in business["tickets"] if t["id"] == payload["ticket_id"])
        return {
            "kind": "reserve",
            "args": {
                "event_id": payload["event_id"],
                "source": deepcopy(row),
                "summary": f"Prior human note for {row['id']}: {row['subject']}; customer update at {row['last_customer_at']}.",
                "mode": mode,
            },
        }

    return action


def busy():
    first = ("Avery", "Jordan", "Morgan", "Casey", "Riley", "Taylor", "Jamie", "Parker")
    last = ("Rivera", "Okafor", "Laurent", "Sato", "Bennett", "Ibrahim")
    subjects = (
        "Export file unavailable",
        "Workspace invitation expired",
        "Receipt address correction",
        "Sign-in verification delayed",
        "Subscription seats missing",
        "Order tracking not updated",
        "Report timezone incorrect",
        "Account ownership transfer",
    )
    tickets = [
        {
            "id": f"T-{601+n}",
            "customer": f"{first[n%8]} {last[n//8]}",
            "subject": subjects[n % 8] + f" for workspace {201+n}",
            "status": "open",
            "last_customer_at": "2026-10-15T08:00:00Z",
            "last_agent_at": None,
        }
        for n in range(48)
    ]
    actions = []
    for n, row in enumerate(tickets):
        group = n // 8
        kind = (
            "resolved"
            if group == 1
            else "agent_reply" if group == 2 else "customer_reply"
        )
        at = (
            "12:00:00"
            if group == 3
            else (
                "11:00:00"
                if group in (1, 2)
                else "09:00:00" if group in (4, 5) else "10:00:00"
            )
        )
        actions.append(
            {
                "kind": "event",
                "args": event(row["id"], f"PRIOR-{n+1:04}", kind, f"2026-10-15T{at}Z"),
            }
        )
    for n in range(32, 48):
        payload = event(
            tickets[n]["id"], f"PRIOR-{n+101:04}", at="2026-10-15T11:30:00Z"
        )
        actions.extend(
            (
                {"kind": "event", "args": payload},
                reserve(payload, "local" if n < 40 else "pipedream"),
            )
        )
        if n >= 40:
            actions.append(
                {
                    "kind": "notification_observation",
                    "args": {
                        "event_id": payload["event_id"],
                        "status": "review_required",
                        "response_sha256": None,
                    },
                }
            )
    for n in range(8):
        actions.append(
            {
                "kind": "event",
                "args": event(
                    tickets[n]["id"],
                    f"PRIOR-{n+201:04}",
                    "customer_reply",
                    "2026-10-15T10:30:00Z",
                ),
            }
        )
    return author(encoded({"tickets": tickets}), actions)


def case(
    ident,
    profile,
    title,
    raw,
    events,
    statuses,
    calls,
    *,
    during=None,
    mode="local",
    action_calls=0,
    after=None,
):
    parsed = parse(raw)
    return {
        "id": ident,
        "profile": profile,
        "title": title,
        "fixture_base64": base64.b64encode(raw).decode(),
        "fixture_sha256": sha256(raw),
        "events": events,
        "during": during,
        "mode": mode,
        "after": after,
        "reference": {
            "statuses": statuses,
            "model_requests": calls,
            "action_requests": action_calls,
            "prior_tickets": len(parsed["business"]["tickets"]),
            "prior_events": len(parsed["business"]["events"]),
            "prior_outbox": len(parsed["business"]["outbox"]),
            "prior_human_steps": len(parsed["history"]),
            "prior_model": None,
            "authentication": None,
            "actual_delivery": None,
        },
    }


def build():
    starter = (ROOT / "fixtures/starter.json").read_bytes()
    tick = event("T-501", "RUN-0001", at="2026-10-15T12:30:00Z")
    repeated = author(
        starter, [{"kind": "event", "args": tick}, reserve(tick, "local")]
    )
    loaded = busy()
    maximum = json.loads(starter)
    maximum["baselineVersion"] = 1
    for row in maximum["tickets"]:
        row["revision"] = 1
    maximum["tickets"][0]["revision"] = MAX_INTEGER - 1
    end = deepcopy(maximum)
    del end["baselineVersion"]
    for row in end["tickets"]:
        del row["revision"]
    end["tickets"][0]["last_customer_at"] = "9999-12-31T23:59:59Z"
    cases = [
        case(
            "starter-due",
            "starter",
            "Unanswered account access",
            starter,
            [tick],
            ["local_queued"],
            1,
        ),
        case(
            "starter-recent",
            "starter",
            "Recent password reset question",
            starter,
            [event("T-504", "RUN-0001", at="2026-10-15T12:30:00Z")],
            ["not_due_or_resolved"],
            0,
        ),
        case(
            "starter-repeat",
            "starter",
            "Preserve an earlier local queue",
            repeated,
            [tick],
            ["already_recorded"],
            0,
        ),
        case(
            "busy-due",
            "busy",
            "Four unanswered tickets among prior support work",
            loaded,
            [event(f"T-{601+n}", f"RUN-{n+1:04}") for n in range(4)],
            ["local_queued"] * 4,
            4,
        ),
        case(
            "busy-repeat",
            "busy",
            "Preserve four previously queued events",
            loaded,
            [
                event(f"T-{633+n}", f"PRIOR-{133+n:04}", at="2026-10-15T11:30:00Z")
                for n in range(4)
            ],
            ["already_recorded"] * 4,
            0,
        ),
        case(
            "busy-answered",
            "busy",
            "Answered tickets amid uncertainty and pending work",
            loaded,
            [event(f"T-{617+n}", f"RUN-{n+1:04}") for n in range(4)],
            ["not_due_or_resolved"] * 4,
            0,
        ),
        case(
            "ambiguous-reply",
            "ambiguous",
            "Agent replies while the note is drafted",
            starter,
            [tick],
            ["changed_before_escalation"],
            1,
            during={
                "phase": "model",
                "event": event(
                    "T-501", "UPDATE-0001", "agent_reply", "2026-10-15T12:31:00Z"
                ),
            },
        ),
        case(
            "ambiguous-resolution",
            "ambiguous",
            "Resolution arrives while the note is drafted",
            starter,
            [tick],
            ["changed_before_escalation"],
            1,
            during={
                "phase": "model",
                "event": event(
                    "T-501", "UPDATE-0001", "resolved", "2026-10-15T12:31:00Z"
                ),
            },
        ),
        case(
            "ambiguous-action-reply",
            "ambiguous",
            "Agent reply during an uncertain notification",
            starter,
            [tick],
            ["failed"],
            1,
            during={
                "phase": "action",
                "event": event(
                    "T-501", "UPDATE-0001", "agent_reply", "2026-10-15T12:31:00Z"
                ),
            },
            mode="pipedream",
            action_calls=1,
        ),
        case(
            "boundary-two-hours",
            "boundary",
            "Exactly two hours unanswered",
            starter,
            [event("T-501", "RUN-0001", at="2026-10-15T12:00:00Z")],
            ["local_queued"],
            1,
        ),
        case(
            "boundary-calendar",
            "boundary",
            "Final calendar second without overflow",
            encoded(end),
            [event("T-501", "RUN-0001", at="9999-12-31T23:59:59Z")],
            ["not_due_or_resolved"],
            0,
        ),
        case(
            "boundary-revision",
            "boundary",
            "Final safe revision preserves earlier queue",
            encoded(maximum),
            [tick],
            ["local_queued"],
            1,
            after={
                "event": event(
                    "T-501", "UPDATE-0001", "agent_reply", "2026-10-15T12:31:00Z"
                ),
                "error": "revision_exhausted",
            },
        ),
    ]
    return starter, encoded(
        {
            "id": "support-populations",
            "version": "1.0.0",
            "synthetic": True,
            "provenance": PROVENANCE,
            "cases": cases,
        }
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    starter, populations = build()
    manifest = f'"""Literal immutable installed dataset pins; regenerated with the authoring script."""\n\nSTARTER_BYTES = {len(starter)}\nSTARTER_SHA256 = "{sha256(starter)}"\nDATASET_BYTES = {len(populations)}\nDATASET_SHA256 = "{sha256(populations)}"\nDATASET_CASES = 12\n'.encode()
    for path, raw in (
        (DATA / "starter.json", starter),
        (DATA / "populations.json", populations),
        (DATA.parent / "_dataset_manifest.py", manifest),
    ):
        if args.check:
            if path.read_bytes() != raw:
                raise ValueError(f"authored population differs: {path.name}")
        else:
            path.write_bytes(raw)
    print(
        json.dumps(
            {
                "cases": 12,
                "bytes": len(populations),
                "sha256": sha256(populations),
                "mode": "checked" if args.check else "authored",
            }
        )
    )


if __name__ == "__main__":
    main()
