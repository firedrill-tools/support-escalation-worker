"""Pinned fictional histories and private references; never runtime capabilities."""

import base64
from copy import deepcopy
from functools import lru_cache

from .assets import read_asset
from ._dataset_manifest import DATASET_CASES, DATASET_SHA256
from .domain import event
from .source import PROVENANCE, parse
from .strict import canonical, digest, keys, parse_json, sha256


def sealed(value):
    return {**deepcopy(value), "content_sha256": digest(value)}


def checked(value):
    if type(value) is not dict or value.get("content_sha256") != digest(
        {k: v for k, v in value.items() if k != "content_sha256"}
    ):
        raise ValueError("original private support control hash differs")
    return value


@lru_cache(maxsize=1)
def _dataset():
    value = parse_json(read_asset("populations.json"))
    keys(value, {"id", "version", "synthetic", "provenance", "cases"})
    if (
        value["id"] != "support-populations"
        or value["version"] != "1.0.0"
        or value["synthetic"] is not True
        or value["provenance"] != PROVENANCE
        or type(value["cases"]) is not list
        or len(value["cases"]) != DATASET_CASES
    ):
        raise ValueError("pinned support population identity differs")
    seen, profiles = set(), {}
    for case in value["cases"]:
        keys(
            case,
            {
                "id",
                "profile",
                "title",
                "fixture_base64",
                "fixture_sha256",
                "events",
                "during",
                "mode",
                "after",
                "reference",
            },
        )
        if case["id"] in seen or case["profile"] not in (
            "starter",
            "busy",
            "ambiguous",
            "boundary",
        ):
            raise ValueError("duplicate or unknown support population")
        seen.add(case["id"])
        profiles[case["profile"]] = profiles.get(case["profile"], 0) + 1
    if set(profiles.values()) != {3}:
        raise ValueError("three support cases in each profile required")
    return value


def dataset():
    return deepcopy(_dataset())


@lru_cache(maxsize=12)
def _baseline(case_id):
    case = next((c for c in _dataset()["cases"] if c["id"] == case_id), None)
    if case is None:
        raise ValueError("unknown pinned support case")
    raw = base64.b64decode(case["fixture_base64"], validate=True)
    if (
        base64.b64encode(raw).decode() != case["fixture_base64"]
        or sha256(raw) != case["fixture_sha256"]
    ):
        raise ValueError("original pinned support fixture differs")
    parsed = parse(raw)
    reference = case["reference"]
    for field, count in (
        ("prior_tickets", len(parsed["business"]["tickets"])),
        ("prior_events", len(parsed["business"]["events"])),
        ("prior_outbox", len(parsed["business"]["outbox"])),
        ("prior_human_steps", len(parsed["history"])),
    ):
        if type(reference[field]) is not int or reference[field] != count:
            raise ValueError("private support metrics contradict original history")
    if (
        reference["prior_model"] is not None
        or reference["authentication"] is not None
        or reference["actual_delivery"] is not None
    ):
        raise ValueError(
            "synthetic history cannot claim prior model/authentication/delivery"
        )
    for payload in case["events"]:
        event(payload)
        if payload["ticket_id"] not in [r["id"] for r in parsed["business"]["tickets"]]:
            raise ValueError("case event refers to an unknown ticket")
    if case["during"] is not None:
        event(case["during"]["event"])
    return case, raw, parsed["business"]


def definition(case_id):
    if type(case_id) is not str:
        raise ValueError("exact pinned support case ID required")
    return deepcopy(_baseline(case_id)[0])


def fixture_bytes(case):
    pinned, raw, _business = _baseline(case["id"])
    if case != pinned:
        raise ValueError("original private case definition differs")
    return raw


def reference(case):
    return {
        "id": "support-populations",
        "version": "1.0.0",
        "case_id": case["id"],
        "dataset_sha256": DATASET_SHA256,
        "definition_sha256": digest(case),
    }


def baseline(value, raw):
    keys(value, {"id", "version", "case_id", "dataset_sha256", "definition_sha256"})
    case, pinned, business = _baseline(value["case_id"])
    if value != reference(case) or raw != pinned:
        raise ValueError("pinned support population reference/fixture differs")
    return deepcopy(business)


def validate_seed(source, original_schema, original_tables, seed):
    checked(seed)
    keys(
        seed,
        {
            "schemaVersion",
            "instance_id",
            "reference",
            "original",
            "original_sha256",
            "context_sha256",
            "provenance",
            "content_sha256",
        },
    )
    if (
        type(seed["schemaVersion"]) is not int
        or seed["schemaVersion"] != 1
        or seed["instance_id"] != source["instance_id"]
        or seed["reference"] != source["population"]
        or seed["provenance"] != PROVENANCE
    ):
        raise ValueError("original population seed identity/provenance differs")
    original = keys(seed["original"], {"schema", "tables"})
    if digest(original) != seed["original_sha256"] or original["schema"] != [
        list(row) for row in original_schema
    ]:
        raise ValueError("original support initialization schema/hash differs")
    keys(original["tables"], set(original_tables))
    previous = {**source, "creation": "fixture", "population": None}
    case, _raw, business = _baseline(source["population"]["case_id"])
    expected = {}
    for table in original_tables:
        if table in ("metadata", "business"):
            raw = canonical(previous if table == "metadata" else business).encode()
            expected[table] = [
                {
                    "id": 1,
                    "document_base64": base64.b64encode(raw).decode(),
                    "document_sha256": sha256(raw),
                }
            ]
        else:
            expected[table] = []
    # The complete original SQL baseline has no imported live scopes or reads.
    # Compare original literal cell bytes, not a caller-rehashed business summary.
    if original["tables"] != expected:
        raise ValueError(
            "population seed cannot import or alter original source authority"
        )
    from .managed_state import context

    if seed["context_sha256"] != context(previous, 0, business)[1]:
        raise ValueError("original support population context differs")
    return seed
