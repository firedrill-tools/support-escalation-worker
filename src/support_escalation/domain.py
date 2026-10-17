"""Pure current ticket/event/outbox policy. Caller/read authority belongs to managed source."""

from __future__ import annotations

from copy import deepcopy
import re

from .strict import digest, identifier, instant, integer, keys, next_revision, text
from .delivery_contract import destination as validate_destination

HUMAN_OBSERVATION = "explicit_synthetic_human_action_observation"
OWNED_OBSERVATION = "owned_sdk_action_observation"
CONTROL_OBSERVATION = "control_uncertainty_observation"
ACK_KINDS = (HUMAN_OBSERVATION, OWNED_OBSERVATION, CONTROL_OBSERVATION)

BASE_FIELDS = {
    "id",
    "customer",
    "subject",
    "status",
    "last_customer_at",
    "last_agent_at",
}
TICKET_FIELDS = BASE_FIELDS | {"revision", "escalated_at"}
EVENT_KINDS = ("clock_tick", "agent_reply", "customer_reply", "resolved")
EVENT_STATES = (
    "eligible",
    "recorded",
    "stale_event",
    "already_resolved",
    "not_due_or_resolved",
    "changed_before_escalation",
    "local_queued",
    "notification_started",
    "remote_acknowledged",
    "review_required",
)


def ticket(value):
    keys(value, TICKET_FIELDS)
    identifier(value["id"])
    text(value["customer"], 1, 200)
    text(value["subject"], 1, 500)
    if type(value["status"]) is not str or value["status"] not in ("open", "resolved"):
        raise ValueError("exact support ticket status required")
    instant(value["last_customer_at"])
    for field in ("last_agent_at", "escalated_at"):
        if value[field] is not None:
            instant(value[field])
    integer(value["revision"], 1)
    # Later replies are retained without reopening this ticket's one escalation.
    return value


def event(value):
    keys(value, {"event_id", "ticket_id", "kind", "occurred_at"})
    identifier(value["event_id"], 100)
    identifier(value["ticket_id"])
    if type(value["kind"]) is not str or value["kind"] not in EVENT_KINDS:
        raise ValueError("exact support event kind required")
    instant(value["occurred_at"])
    return value


def eligible(row, now):
    ticket(row)
    at, customer = instant(now), instant(row["last_customer_at"])
    agent = instant(row["last_agent_at"]) if row["last_agent_at"] else None
    # Subtraction avoids overflow near year9999. The explicit clock does not advance wall time.
    return (
        row["status"] == "open"
        and row["escalated_at"] is None
        and (agent is None or agent < customer)
        and at >= customer
        and (at - customer).total_seconds() >= 7200
    )


def initial(value):
    keys(value, {"tickets"})
    if type(value["tickets"]) is not list or not 1 <= len(value["tickets"]) <= 128:
        raise ValueError("nonempty bounded support ticket fixture required")
    rows = []
    for row in value["tickets"]:
        keys(row, BASE_FIELDS)
        rows.append(ticket({**deepcopy(row), "revision": 1, "escalated_at": None}))
    result = {"tickets": rows, "events": [], "outbox": []}
    validate(result)
    return result


def validate(value):
    keys(value, {"tickets", "events", "outbox"})
    for field, limit in (("tickets", 128), ("events", 1024), ("outbox", 512)):
        if type(value[field]) is not list or len(value[field]) > limit:
            raise ValueError("complete bounded support tables required")
    ids = [ticket(row)["id"] for row in value["tickets"]]
    if not ids or len(set(ids)) != len(ids):
        raise ValueError("nonempty unique support ticket identity required")
    seen = {}
    for row in value["events"]:
        keys(row, {"payload", "source", "status", "result"})
        payload, source = event(row["payload"]), ticket(row["source"])
        if (
            payload["event_id"] in seen
            or payload["ticket_id"] != source["id"]
            or source["id"] not in ids
            or type(row["status"]) is not str
            or row["status"] not in EVENT_STATES
            or type(row["result"]) is not dict
            or row["result"].get("status") != row["status"]
        ):
            raise ValueError("original support event linkage differs")
        seen[payload["event_id"]] = row
    delivered = set()
    for row in value["outbox"]:
        keys(
            row,
            {
                "ticket_id",
                "event_id",
                "summary",
                "source",
                "source_sha256",
                "event",
                "mode",
                "status",
                "acknowledgement",
                "destination",
            },
        )
        source = ticket(row["source"])
        payload = event(row["event"])
        text(row["summary"], 1, 4096)
        old = seen.get(row["event_id"])
        if (
            row["ticket_id"] in delivered
            or row["ticket_id"] != source["id"]
            or row["event_id"] != payload["event_id"]
            or payload["ticket_id"] != source["id"]
            or payload["kind"] != "clock_tick"
            or row["source_sha256"] != digest(source)
            or not eligible(source, payload["occurred_at"])
            or old is None
            or old["source"] != source
            or old["payload"] != payload
            or old["status"] != row["status"]
        ):
            raise ValueError("unsupported original support outbox snapshot")
        if row["mode"] == "local":
            if (
                row["status"] != "local_queued"
                or row["acknowledgement"] is not None
                or row["destination"] is not None
            ):
                raise ValueError("local queue cannot claim remote delivery")
        elif row["mode"] == "pipedream":
            if row["destination"] is not None:
                validate_destination(row["destination"])
            if row["status"] not in (
                "notification_started",
                "remote_acknowledged",
                "review_required",
            ):
                raise ValueError("exact remote acknowledgement state required")
            if (
                row["status"] == "notification_started"
                and row["acknowledgement"] is not None
            ):
                raise ValueError("pending remote action cannot claim acknowledgement")
            if row["status"] != "notification_started":
                keys(
                    row["acknowledgement"],
                    {"kind", "response_sha256", "actual_delivery"},
                )
                if (
                    row["acknowledgement"]["kind"] not in ACK_KINDS
                    or row["acknowledgement"]["actual_delivery"] is not None
                    or (
                        row["acknowledgement"]["response_sha256"] is not None
                        and (
                            type(row["acknowledgement"]["response_sha256"]) is not str
                            or not re.fullmatch(
                                r"[0-9a-f]{64}",
                                row["acknowledgement"]["response_sha256"],
                            )
                        )
                    )
                ):
                    raise ValueError("remote delivery remains unverified")
                if row["acknowledgement"]["kind"] == OWNED_OBSERVATION and (
                    row["status"] != "remote_acknowledged"
                    or row["destination"] is None
                    or row["acknowledgement"]["response_sha256"] is None
                ):
                    raise ValueError(
                        "owned action acknowledgement needs its exact destination and original response"
                    )
                if (
                    row["acknowledgement"]["kind"] == CONTROL_OBSERVATION
                    and row["status"] != "review_required"
                ):
                    raise ValueError("control observation can only record uncertainty")
        else:
            raise ValueError("explicit local or pipedream mode required")
        delivered.add(row["ticket_id"])
    for row in value["tickets"]:
        if (row["escalated_at"] is not None) != (row["id"] in delivered):
            raise ValueError("current escalation must have its linked outbox")
        if row["id"] in delivered:
            outbox = next(b for b in value["outbox"] if b["ticket_id"] == row["id"])
            if (
                row["escalated_at"] != outbox["event"]["occurred_at"]
                or row["revision"] <= outbox["source"]["revision"]
            ):
                raise ValueError("current escalation revision/time differs")
    return value


def transition(business, action):
    validate(business)
    keys(action, {"kind", "args"})
    state = deepcopy(business)
    args = action["args"]
    if action["kind"] == "event":
        payload = event(args)
        previous = next(
            (
                e
                for e in state["events"]
                if e["payload"]["event_id"] == payload["event_id"]
            ),
            None,
        )
        if previous:
            if previous["payload"] != payload:
                raise ValueError("event_id_reused_with_different_payload")
            return state, deepcopy(previous["result"])
        row = next(
            (t for t in state["tickets"] if t["id"] == payload["ticket_id"]), None
        )
        if row is None:
            raise ValueError("unknown_ticket")
        before, kind, at = (
            deepcopy(row),
            payload["kind"],
            instant(payload["occurred_at"]),
        )
        if kind == "clock_tick":
            result = {
                "status": (
                    "eligible"
                    if eligible(row, payload["occurred_at"])
                    else "not_due_or_resolved"
                ),
                "ticket_id": row["id"],
            }
        elif row["status"] == "resolved":
            result = {"status": "already_resolved"}
        else:
            field = {
                "agent_reply": "last_agent_at",
                "customer_reply": "last_customer_at",
            }.get(kind)
            stale = (
                field is not None
                and row[field] is not None
                and at <= instant(row[field])
            ) or (
                kind == "resolved"
                and at
                < max(
                    instant(row["last_customer_at"]),
                    (
                        instant(row["last_agent_at"])
                        if row["last_agent_at"]
                        else instant(row["last_customer_at"])
                    ),
                )
            )
            if stale:
                result = {"status": "stale_event"}
            else:
                revision = next_revision(row["revision"])
                if field:
                    row[field] = payload["occurred_at"]
                else:
                    row["status"] = "resolved"
                row["revision"] = revision
                result = {"status": "recorded", "revision": revision}
        state["events"].append(
            {
                "payload": deepcopy(payload),
                "source": before,
                "status": result["status"],
                "result": deepcopy(result),
            }
        )
    elif action["kind"] == "reserve":
        keys(
            args,
            {"event_id", "source", "summary", "mode"}
            | (
                {"destination"}
                if type(args) is dict and "destination" in args
                else set()
            ),
        )
        target = args.get("destination")
        if target is not None:
            validate_destination(target)
        if args["mode"] == "local" and target is not None:
            raise ValueError("local queue cannot select remote destination")
        identifier(args["event_id"], 100)
        ticket(args["source"])
        text(args["summary"], 1, 4096)
        if not args["summary"].strip() or args["mode"] not in ("local", "pipedream"):
            raise ValueError("nonblank draft and explicit action mode required")
        entry = next(
            (
                e
                for e in state["events"]
                if e["payload"]["event_id"] == args["event_id"]
            ),
            None,
        )
        if entry is None or entry["payload"]["kind"] != "clock_tick":
            raise ValueError("original clock event required")
        if entry["status"] != "eligible":
            return state, deepcopy(entry["result"])
        payload = entry["payload"]
        row = next(t for t in state["tickets"] if t["id"] == payload["ticket_id"])
        if (
            row != args["source"]
            or entry["source"] != args["source"]
            or not eligible(row, payload["occurred_at"])
        ):
            result = {"status": "changed_before_escalation"}
        else:
            revision = next_revision(row["revision"])
            status = (
                "local_queued" if args["mode"] == "local" else "notification_started"
            )
            state["outbox"].append(
                {
                    "ticket_id": row["id"],
                    "event_id": args["event_id"],
                    "summary": args["summary"],
                    "source": deepcopy(row),
                    "source_sha256": digest(row),
                    "event": deepcopy(payload),
                    "mode": args["mode"],
                    "status": status,
                    "acknowledgement": None,
                    "destination": deepcopy(target),
                }
            )
            row["escalated_at"], row["revision"] = payload["occurred_at"], revision
            result = {"status": status, "ticket_id": row["id"]}
        entry["status"], entry["result"] = result["status"], deepcopy(result)
    elif action["kind"] == "notification_observation":
        keys(
            args,
            {"event_id", "status", "response_sha256"}
            | (
                {"observation_kind"}
                if type(args) is dict and "observation_kind" in args
                else set()
            ),
        )
        observation_kind = args.get("observation_kind", HUMAN_OBSERVATION)
        if observation_kind not in ACK_KINDS:
            raise ValueError("declared action observation provenance required")
        identifier(args["event_id"], 100)
        if args["status"] not in ("remote_acknowledged", "review_required"):
            raise ValueError("explicit remote acknowledgement or uncertainty required")
        row = next(
            (o for o in state["outbox"] if o["event_id"] == args["event_id"]), None
        )
        if row is None or row["mode"] != "pipedream":
            raise ValueError("original remote reservation required")
        if row["status"] != "notification_started":
            raise ValueError("remote notification must never replay")
        row["status"] = args["status"]
        row["acknowledgement"] = {
            "kind": observation_kind,
            "response_sha256": args["response_sha256"],
            "actual_delivery": None,
        }
        result = {"status": args["status"], "ticket_id": row["ticket_id"]}
        entry = next(
            e for e in state["events"] if e["payload"]["event_id"] == args["event_id"]
        )
        entry["status"], entry["result"] = result["status"], deepcopy(result)
    else:
        raise ValueError("unknown support action")
    validate(state)
    return state, result
