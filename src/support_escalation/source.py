"""Complete original synthetic fixture and deterministic human action replay."""

from __future__ import annotations

import base64
from copy import deepcopy

from .domain import (
    HUMAN_OBSERVATION,
    initial,
    transition,
    validate as validate_business,
)
from .strict import digest, keys, parse_json, sha256

PROVENANCE = {
    "kind": "explicit_synthetic_human_source",
    "prior_model": None,
    "authentication": None,
    "actual_delivery": None,
}


def validate(data):
    if type(data) is dict and set(data) in (
        {"tickets"},
        {"tickets", "baselineVersion"},
    ):
        return {"business": initial(data), "origin": None, "history": []}
    keys(data, {"schemaVersion", "origin", "business", "history"})
    if type(data["schemaVersion"]) is not int or data["schemaVersion"] != 1:
        raise ValueError("support source schemaVersion must be1")
    origin = data["origin"]
    keys(origin, {"fixture_base64", "fixture_sha256", "provenance"})
    if type(origin["fixture_base64"]) is not str:
        raise ValueError("literal original support fixture required")
    try:
        raw = base64.b64decode(origin["fixture_base64"], validate=True)
    except Exception as error:
        raise ValueError("original support fixture encoding differs") from error
    if (
        base64.b64encode(raw).decode() != origin["fixture_base64"]
        or sha256(raw) != origin["fixture_sha256"]
        or origin["provenance"] != PROVENANCE
    ):
        raise ValueError("original support fixture provenance/hash differs")
    current = initial(parse_json(raw))
    if type(data["history"]) is not list or not 1 <= len(data["history"]) <= 2048:
        raise ValueError("complete bounded support source history required")
    for index, step in enumerate(data["history"], 1):
        keys(step, {"ordinal", "action", "before_sha256", "after_sha256", "result"})
        if (
            type(step["ordinal"]) is not int
            or step["ordinal"] != index
            or step["before_sha256"] != digest(current)
        ):
            raise ValueError("support original history frontier differs")
        keys(step["action"], {"kind", "args"})
        if (
            step["action"]["kind"] == "notification_observation"
            and type(step["action"]["args"]) is dict
            and step["action"]["args"].get("observation_kind", HUMAN_OBSERVATION)
            != HUMAN_OBSERVATION
        ):
            raise ValueError(
                "synthetic-human fixture cannot import native/control observation authority"
            )
        after, result = transition(current, step["action"])
        if (
            after == current
            or step["after_sha256"] != digest(after)
            or step["result"] != result
        ):
            raise ValueError("support original history transition/result differs")
        current = after
    validate_business(data["business"])
    if current != data["business"]:
        raise ValueError(
            "current support source differs from complete original history"
        )
    return deepcopy(data)


def parse(raw):
    return validate(parse_json(raw))


def apply(raw, action):
    before = parse(raw)
    business, result = transition(before["business"], action)
    if business == before["business"]:
        return None, result
    origin = before["origin"] or {
        "fixture_base64": base64.b64encode(raw).decode(),
        "fixture_sha256": sha256(raw),
        "provenance": deepcopy(PROVENANCE),
    }
    state = {
        "schemaVersion": 1,
        "origin": origin,
        "business": business,
        "history": [
            *before["history"],
            {
                "ordinal": len(before["history"]) + 1,
                "action": deepcopy(action),
                "before_sha256": digest(before["business"]),
                "after_sha256": digest(business),
                "result": deepcopy(result),
            },
        ],
    }
    validate(state)
    return state, result
