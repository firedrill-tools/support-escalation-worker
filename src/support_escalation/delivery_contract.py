"""Pure destination/acknowledgement contract; no provider, credential or source effects."""

import re
from urllib.parse import urlsplit

from .strict import keys, text


def origin(value):
    text(value, 1, 512)
    if any(ord(c) < 33 or ord(c) > 126 for c in value):
        raise ValueError("canonical provider origin required")
    p = urlsplit(value)
    port = p.port
    if (
        p.scheme not in ("http", "https")
        or p.hostname is None
        or p.username
        or p.password
        or p.query
        or p.fragment
        or p.path not in ("", "/")
    ):
        raise ValueError("exact HTTPS or owned loopback origin required")
    if p.scheme == "http" and (p.hostname != "127.0.0.1" or port is None):
        raise ValueError("HTTP is limited to explicit owned loopback")
    canonical = (
        p.scheme
        + "://"
        + p.hostname
        + (
            ":" + str(port)
            if port is not None and port != {"http": 80, "https": 443}[p.scheme]
            else ""
        )
    )
    if value.rstrip("/") != canonical:
        raise ValueError("canonical original provider origin required")
    return canonical


def atom(value, maximum=200):
    text(value, 1, maximum)
    if any(ord(c) < 33 or ord(c) > 126 for c in value):
        raise ValueError("single-line original destination identifier required")
    return value


def destination(value):
    keys(
        value,
        {
            "provider",
            "base_url",
            "project_environment",
            "project_id",
            "external_user_id",
            "auth_provision_id",
            "channel",
            "credential_sha256",
        },
    )
    if (
        value["provider"] != "pipedream"
        or value["project_environment"] != "development"
    ):
        raise ValueError("explicit Pipedream development destination required")
    origin(value["base_url"])
    for name in ("project_id", "auth_provision_id"):
        if type(value[name]) is not str or not re.fullmatch(
            r"[A-Za-z0-9_-]{1,128}", value[name]
        ):
            raise ValueError("canonical Pipedream project/account identifier required")
    atom(value["external_user_id"])
    if type(value["channel"]) is not str or not re.fullmatch(
        r"[CDG][A-Z0-9]{1,63}", value["channel"]
    ):
        raise ValueError("explicit canonical Slack channel ID required")
    if type(value["credential_sha256"]) is not str or not re.fullmatch(
        r"[0-9a-f]{64}", value["credential_sha256"]
    ):
        raise ValueError("frozen Pipedream credential fingerprint required")
    return value


def acknowledgement(body, target):
    destination(target)
    if type(body) is not dict or "error" in body or type(body.get("ret")) is not dict:
        raise ValueError("complete original Pipedream action return required")
    result = body["ret"]
    if (
        result.get("ok") is not True
        or "error" in result
        or result.get("channel") != target["channel"]
        or type(result.get("ts")) is not str
        or not re.fullmatch(r"[0-9]{1,16}\.[0-9]{6}", result["ts"])
    ):
        raise ValueError(
            "original action must explicitly acknowledge the exact channel and message timestamp"
        )
    return {
        "status": "remote_acknowledged",
        "reported_channel": result["channel"],
        "reported_message_ts": result["ts"],
        "actual_delivery": None,
    }
