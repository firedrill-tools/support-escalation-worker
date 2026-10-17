"""Check complete original Anthropic Messages JSON before SDK normalization."""

from .strict import integer, parse_json, text

MAX_RESPONSE_BYTES = 1024 * 1024


def completed(raw):
    if len(raw) > MAX_RESPONSE_BYTES:
        raise ValueError("original Anthropic response exceeds1MiB")
    body = parse_json(raw)
    if (
        type(body) is not dict
        or "error" in body
        or body.get("type") != "message"
        or body.get("role") != "assistant"
        or body.get("stop_reason") != "end_turn"
        or "stop_sequence" not in body
        or body["stop_sequence"] is not None
    ):
        raise ValueError("complete original Anthropic assistant end_turn required")
    text(body.get("id"), 1, 240)
    text(body.get("model"), 1, 240)
    parts = body.get("content")
    if type(parts) is not list or not 1 <= len(parts) <= 32:
        raise ValueError("bounded original Anthropic content required")
    texts = []
    for part in parts:
        if type(part) is not dict:
            raise ValueError("original Anthropic content object required")
        kind = part.get("type")
        if kind == "text":
            text(part.get("text"), 0, 4096)
            citations = part.get("citations")
            if citations is not None and (
                type(citations) is not list or len(citations) > 32
            ):
                raise ValueError("bounded original citation metadata required")
            texts.append(part["text"])
        elif kind == "thinking":
            text(part.get("thinking"), 0, 16384)
            text(part.get("signature"), 1, 16384)
        elif kind == "redacted_thinking":
            text(part.get("data"), 1, 65536)
        else:
            raise ValueError("tool-free original support summary required")
    summary = " ".join(texts).strip()
    text(summary, 1, 4096)
    return body, {
        "summary": summary,
        "response_id": body["id"],
        "reported_model": body["model"],
        "stop_reason": "end_turn",
    }


def counter(usage, name):
    if type(usage) is not dict or name not in usage:
        return {"status": "unknown", "value": None}
    value = usage[name]
    try:
        integer(value)
    except ValueError:
        return {"status": "invalid", "value": None}
    return {"status": "observed", "value": value}


def usage_of(body):
    usage = body.get("usage") if type(body) is dict else None
    return {
        "source": "original Anthropic JSON counters; no estimate or billing proof",
        **{
            name: counter(usage, name)
            for name in (
                "input_tokens",
                "output_tokens",
                "cache_creation_input_tokens",
                "cache_read_input_tokens",
            )
        },
        "cost_usd": None,
    }
