"""Exact SQLite source images and replay of scoped support observations/decisions."""

from __future__ import annotations

import base64
import re
from copy import deepcopy
from uuid import UUID

from .domain import (
    CONTROL_OBSERVATION,
    OWNED_OBSERVATION,
    eligible,
    event,
    ticket,
    transition,
)
from .delivery_contract import destination as validate_destination
from .source import PROVENANCE, parse
from .strict import canonical, digest, integer, keys, parse_json, sha256, text

APPLICATION_ID = 0x46445345
SOURCE_VERSION = 2
MAX_DATABASE_BYTES = 64 * 1024 * 1024
SCHEMA = {
    "metadata": "CREATE TABLE metadata (id INTEGER PRIMARY KEY CHECK(id=1), document TEXT NOT NULL) STRICT",
    "business": "CREATE TABLE business (id INTEGER PRIMARY KEY CHECK(id=1), document TEXT NOT NULL) STRICT",
    "operations": "CREATE TABLE operations (id TEXT PRIMARY KEY, document TEXT NOT NULL) STRICT",
    "observations": "CREATE TABLE observations (id INTEGER PRIMARY KEY, document TEXT NOT NULL) STRICT",
    "decisions": "CREATE TABLE decisions (id INTEGER PRIMARY KEY, document TEXT NOT NULL) STRICT",
    "seed_state": "CREATE TABLE seed_state (id INTEGER PRIMARY KEY CHECK(id=1), document TEXT NOT NULL) STRICT",
}
LIMITS = {
    "metadata": 1,
    "business": 1,
    "operations": 128,
    "observations": 1024,
    "decisions": 512,
    "seed_state": 1,
}


def uuid(value):
    if type(value) is not str or str(UUID(value)) != value or UUID(value).version != 4:
        raise ValueError("original UUID4 support identity required")
    return value


def hex_digest(value):
    if type(value) is not str or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError("original SHA256 required")
    return value


def schema(db):
    if (
        db.execute("PRAGMA application_id").fetchone()[0] != APPLICATION_ID
        or db.execute("PRAGMA user_version").fetchone()[0] != SOURCE_VERSION
        or db.execute("PRAGMA page_size").fetchone()[0] != 4096
        or db.execute("PRAGMA encoding").fetchone()[0] != "UTF-8"
        or db.execute("PRAGMA journal_mode").fetchone()[0] != "delete"
    ):
        raise ValueError("original support database header differs")
    if db.execute("PRAGMA page_count").fetchone()[0] * 4096 > MAX_DATABASE_BYTES:
        raise ValueError("support database exceeds64MiB")
    actual = [
        tuple(row)
        for row in db.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
        )
    ]
    expected = sorted(
        [("table", name, name, sql) for name, sql in SCHEMA.items()]
        + [("index", "sqlite_autoindex_operations_1", "operations", None)]
    )
    if actual != expected or [
        tuple(r) for r in db.execute("PRAGMA quick_check(1)")
    ] != [("ok",)]:
        raise ValueError("exact original support SQL schema/integrity required")
    return actual


def rows(db, name):
    count = db.execute("SELECT count(*) FROM " + name).fetchone()[0]
    if count > LIMITS[name]:
        raise ValueError("bounded complete support SQL rows required")
    original = []
    values = []
    for identity, raw in db.execute(
        "SELECT id,CAST(document AS BLOB) FROM " + name + " ORDER BY id"
    ):
        if type(raw) is not bytes:
            raise ValueError("original support SQL text bytes required")
        value = parse_json(raw)
        if raw.decode("utf-8", errors="strict").encode("utf-8", errors="strict") != raw:
            raise ValueError("original support SQL UTF8 differs")
        original.append(
            {
                "id": identity,
                "document_base64": base64.b64encode(raw).decode(),
                "document_sha256": sha256(raw),
            }
        )
        values.append((identity, value))
    return original, values


def header(value, physical, instance_id=None):
    keys(
        value,
        {
            "schemaVersion",
            "kind",
            "instance_id",
            "physical_identity",
            "fixture_base64",
            "fixture_sha256",
            "provenance",
            "creation",
            "population",
        },
    )
    if (
        type(value["schemaVersion"]) is not int
        or value["schemaVersion"] != SOURCE_VERSION
        or value["kind"] != "managed_synthetic_support_source"
        or value["physical_identity"] != physical
        or value["provenance"] != PROVENANCE
        or value["creation"] not in ("fixture", "population")
        or value["creation"] == "fixture"
        and value["population"] is not None
    ):
        raise ValueError("original support source header identity/provenance differs")
    uuid(value["instance_id"])
    if instance_id is not None and value["instance_id"] != instance_id:
        raise ValueError("original support source UUID differs")
    if type(value["fixture_base64"]) is not str:
        raise ValueError("literal support initial fixture required")
    raw = base64.b64decode(value["fixture_base64"], validate=True)
    if (
        base64.b64encode(raw).decode() != value["fixture_base64"]
        or sha256(raw) != value["fixture_sha256"]
    ):
        raise ValueError("original support fixture hash/encoding differs")
    if value["creation"] == "population":
        from .populations import baseline

        return baseline(value["population"], raw)
    return parse(raw)["business"]


def context(source, frontier, business):
    value = {
        "source_sha256": digest(source),
        "frontier": frontier,
        "business_sha256": digest(business),
    }
    return value, digest(value)


def caller_intent(business, payload, context_sha256):
    event(payload)
    row = next(
        (r for r in business["tickets"] if r["id"] == payload["ticket_id"]), None
    )
    if row is None:
        raise ValueError("unknown caller ticket")
    return {
        "event": deepcopy(payload),
        "ticket": deepcopy(row),
        "context_sha256": hex_digest(context_sha256),
    }


def check_operation(value):
    keys(
        value,
        {
            "id",
            "token_sha256",
            "task_sha256",
            "intent",
            "initial_frontier",
            "allow_writes",
            "mode",
            "destination",
            "status",
            "closed_frontier",
            "closed_observations",
        },
    )
    uuid(value["id"])
    hex_digest(value["token_sha256"])
    hex_digest(value["task_sha256"])
    integer(value["initial_frontier"])
    if (
        type(value["allow_writes"]) is not bool
        or value["mode"] not in ("local", "pipedream")
        or value["status"] not in ("active", "closed")
    ):
        raise ValueError("exact support operation/write/mode/status required")
    keys(value["intent"], {"event", "ticket", "context_sha256"})
    if value["mode"] == "local":
        if value["destination"] is not None:
            raise ValueError("local scope cannot authorize remote destination")
    else:
        validate_destination(value["destination"])
    if value["status"] == "active":
        if (
            value["closed_frontier"] is not None
            or value["closed_observations"] is not None
        ):
            raise ValueError("active support scope cannot claim closure")
    else:
        integer(value["closed_frontier"])
        integer(value["closed_observations"])
        if value["closed_frontier"] < value["initial_frontier"]:
            raise ValueError("support scope closure precedes registration")
    event(value["intent"]["event"])
    ticket(value["intent"]["ticket"])
    hex_digest(value["intent"]["context_sha256"])
    if value["intent"]["event"]["ticket_id"] != value["intent"]["ticket"]["id"]:
        raise ValueError("complete caller ticket/event identity differs")
    return value


def public_read(business, operation):
    payload = operation["intent"]["event"]
    row = next(r for r in business["tickets"] if r["id"] == payload["ticket_id"])
    previous = next(
        (
            r
            for r in business["events"]
            if r["payload"]["event_id"] == payload["event_id"]
        ),
        None,
    )
    return {
        "ticket": deepcopy(row),
        "event": deepcopy(payload),
        "eligible": eligible(row, payload["occurred_at"]),
        "previous_event_status": previous["status"] if previous else None,
    }


def permitted_action(operation, action, observation):
    keys(action, {"kind", "args"})
    payload = operation["intent"]["event"]
    args = action["args"]
    if action["kind"] == "event":
        event(args)
        if args != payload:
            raise ValueError("action differs from complete caller event intent")
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
        ticket(args["source"])
        text(args["summary"], 1, 4096)
        if (
            payload["kind"] != "clock_tick"
            or args["event_id"] != payload["event_id"]
            or args["mode"] != operation["mode"]
            or args["source"] != observation["value"]["ticket"]
            or args.get("destination") != operation["destination"]
        ):
            raise ValueError(
                "draft differs from own current read/caller notification intent"
            )
    elif action["kind"] == "notification_observation":
        keys(args, {"event_id", "status", "response_sha256", "observation_kind"})
        if (
            payload["kind"] != "clock_tick"
            or operation["mode"] != "pipedream"
            or args["event_id"] != payload["event_id"]
            or args["observation_kind"] not in (OWNED_OBSERVATION, CONTROL_OBSERVATION)
        ):
            raise ValueError("notification differs from caller remote reservation")
    else:
        raise ValueError("unsupported scoped support action")


def snapshot(db, physical, instance_id=None):
    original_schema = schema(db)
    raw = {}
    table = {}
    for name in SCHEMA:
        raw[name], table[name] = rows(db, name)
    if [r[0] for r in table["metadata"]] != [1] or [
        r[0] for r in table["business"]
    ] != [1]:
        raise ValueError("original support singleton/fixture seed differs")
    source = table["metadata"][0][1]
    current = header(source, physical, instance_id)
    if source["creation"] == "population":
        from .populations import validate_seed

        if [r[0] for r in table["seed_state"]] != [1]:
            raise ValueError("original support population seed required")
        validate_seed(source, original_schema, raw, table["seed_state"][0][1])
    elif table["seed_state"]:
        raise ValueError("fixture cannot import population control")
    operations = {}
    for identity, value in table["operations"]:
        check_operation(value)
        if identity != value["id"]:
            raise ValueError("support operation SQL identity differs")
        operations[identity] = value
    if sum(v["status"] == "active" for v in operations.values()) > 1:
        raise ValueError("one active support operation required")
    observations = {}
    for expected, (identity, value) in enumerate(table["observations"], 1):
        keys(
            value,
            {
                "id",
                "operation_id",
                "frontier",
                "context_sha256",
                "value",
                "proof_sha256",
            },
        )
        if (
            type(identity) is not int
            or identity != expected
            or type(value["id"]) is not int
            or value["id"] != identity
            or value["operation_id"] not in operations
        ):
            raise ValueError("support observation SQL/owner identity differs")
        integer(value["frontier"])
        hex_digest(value["context_sha256"])
        hex_digest(value["proof_sha256"])
        observations[identity] = value
    frontiers = [deepcopy(current)]
    used = set()
    decisions = []
    for ordinal, (identity, value) in enumerate(table["decisions"], 1):
        keys(
            value,
            {
                "id",
                "action",
                "operation_id",
                "read_id",
                "before_context_sha256",
                "after_context_sha256",
                "status",
                "result",
                "provenance",
            },
        )
        if (
            type(identity) is not int
            or identity != ordinal
            or type(value["id"]) is not int
            or value["id"] != ordinal
        ):
            raise ValueError("support decision frontier differs")
        _, before = context(source, ordinal - 1, current)
        if value["before_context_sha256"] != before:
            raise ValueError("support decision original before context differs")
        if value["operation_id"] is None:
            if (
                value["read_id"] is not None
                or value["provenance"] != "explicit_synthetic_human_event"
                or value["action"].get("kind") != "event"
            ):
                raise ValueError("human source may only author explicit support events")
            after, result = transition(current, value["action"])
            status = "applied" if after != current else "unchanged"
        else:
            if (
                type(value["operation_id"]) is not str
                or type(value["read_id"]) is not int
            ):
                raise ValueError("exact scoped support decision identity required")
            operation = operations.get(value["operation_id"])
            read = observations.get(value["read_id"])
            if (
                operation is None
                or read is None
                or value["read_id"] in used
                or read["operation_id"] != operation["id"]
                or read["frontier"] != ordinal - 1
                or read["context_sha256"] != before
                or value["provenance"] != "managed_caller"
                or operation["initial_frontier"] > ordinal - 1
                or operation["status"] == "closed"
                and ordinal > operation["closed_frontier"]
            ):
                raise ValueError(
                    "genuine own current unused support observation required"
                )
            permitted_action(operation, value["action"], read)
            used.add(value["read_id"])
            if not operation["allow_writes"]:
                after, result, status = (
                    current,
                    {"status": "write_permission_required"},
                    "denied",
                )
            else:
                after, result = transition(current, value["action"])
                status = "applied" if after != current else "unchanged"
        _, after_context = context(source, ordinal, after)
        if (
            value["result"] != result
            or value["status"] != status
            or value["after_context_sha256"] != after_context
        ):
            raise ValueError("support decision replay/result differs")
        current = after
        frontiers.append(deepcopy(current))
        decisions.append(value)
    for operation in operations.values():
        frontier = operation["initial_frontier"]
        if frontier >= len(frontiers):
            raise ValueError("support initial caller frontier is absent")
        if operation["status"] == "closed" and (
            operation["closed_frontier"] >= len(frontiers)
            or operation["closed_observations"] > len(observations)
        ):
            raise ValueError("support recorded closure frontier is absent")
        _, ctx = context(source, frontier, frontiers[frontier])
        if operation["intent"] != caller_intent(
            frontiers[frontier], operation["intent"]["event"], ctx
        ):
            raise ValueError("support complete initial caller intent differs")
    for observation in observations.values():
        frontier = observation["frontier"]
        operation = operations[observation["operation_id"]]
        if (
            frontier >= len(frontiers)
            or frontier < operation["initial_frontier"]
            or operation["status"] == "closed"
            and (
                frontier > operation["closed_frontier"]
                or observation["id"] > operation["closed_observations"]
            )
        ):
            raise ValueError("support observation frontier is absent")
        _, ctx = context(source, frontier, frontiers[frontier])
        if observation["context_sha256"] != ctx or observation["value"] != public_read(
            frontiers[frontier], operation
        ):
            raise ValueError("support observation original source value differs")
    if table["business"][0][1] != current:
        raise ValueError("complete support business replay differs from original SQL")
    ctx, ctx_sha = context(source, len(decisions), current)
    original = {"schema": original_schema, "tables": raw}
    return {
        "source": deepcopy(source),
        "business": deepcopy(current),
        "records": {
            "operations": list(operations.values()),
            "observations": list(observations.values()),
            "decisions": decisions,
        },
        "context": ctx,
        "context_sha256": ctx_sha,
        "sha256": digest(original),
        "original": original,
    }
