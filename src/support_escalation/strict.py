"""Bounded original JSON and physical file identity, with no source repair."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import stat
from datetime import datetime, timezone
from pathlib import Path

MAX_BYTES = 4 * 1024 * 1024
MAX_INTEGER = 2**53 - 1


def canonical(value):
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def digest(value):
    return sha256(canonical(value).encode("utf-8", errors="strict"))


def keys(value, fields):
    if type(value) is not dict or set(value) != set(fields):
        raise ValueError("exact original support fields required")
    return value


def text(value, minimum=1, maximum=200):
    if (
        type(value) is not str
        or not minimum <= len(value) <= maximum
        or "\x00" in value
    ):
        raise ValueError("bounded original support text required")
    value.encode("utf-8", errors="strict")
    return value


def identifier(value, maximum=64):
    if type(value) is not str or not re.fullmatch(
        r"[A-Z][A-Z0-9_-]{0," + str(maximum - 1) + r"}", value
    ):
        raise ValueError("canonical support identifier required")
    return value


def integer(value, minimum=0):
    if type(value) is not int or not minimum <= value <= MAX_INTEGER:
        raise ValueError("exact safe support integer required")
    return value


def next_revision(value):
    if integer(value, 1) == MAX_INTEGER:
        raise ValueError("revision_exhausted")
    return value + 1


def instant(value):
    if type(value) is not str or not re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z", value
    ):
        raise ValueError("canonical UTC second timestamp required")
    return datetime.fromisoformat(value[:-1] + "+00:00").astimezone(timezone.utc)


def parse_json(raw):
    if type(raw) is not bytes or not 0 < len(raw) <= MAX_BYTES:
        raise ValueError("bounded complete original support JSON required")

    def pairs(rows):
        result = {}
        for key, value in rows:
            if key in result:
                raise ValueError("duplicate original JSON field")
            result[key] = value
        return result

    def number(token):
        result = float(token) if any(c in token for c in ".eE") else int(token)
        if (
            (type(result) is float and not math.isfinite(result))
            or result == 0
            and token.startswith("-")
        ):
            raise ValueError("nonfinite or negative-zero JSON number")
        return result

    try:
        result = json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=pairs,
            parse_int=number,
            parse_float=number,
            parse_constant=lambda _: (_ for _ in ()).throw(
                ValueError("nonfinite JSON number")
            ),
        )
    except RecursionError as error:
        raise ValueError("original JSON nesting exceeds bound") from error
    pending, count = [(result, 0)], 0
    while pending:
        value, depth = pending.pop()
        count += 1
        if depth > 64 or count > 200000:
            raise ValueError("original JSON structure exceeds bound")
        if type(value) is str:
            text(value, 0, MAX_BYTES)
        elif type(value) is dict:
            pending.extend((k, depth + 1) for k in value)
            pending.extend((v, depth + 1) for v in value.values())
        elif type(value) is list:
            pending.extend((v, depth + 1) for v in value)
    return result


def identity(st):
    return (
        st.st_dev,
        st.st_ino,
        st.st_size,
        st.st_nlink,
        st.st_mtime_ns,
        st.st_ctime_ns,
    )


def read_original(path):
    path = Path(path)
    before = path.lstat()
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_nlink != 1
        or not 0 < before.st_size <= MAX_BYTES
    ):
        raise ValueError("bounded regular unshared nonsymlink support source required")
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        opened = os.fstat(fd)
        with os.fdopen(os.dup(fd), "rb") as stream:
            raw = stream.read(MAX_BYTES + 1)
        after, current = os.fstat(fd), path.lstat()
        if (
            identity(before) != identity(opened)
            or identity(opened) != identity(after)
            or identity(after) != identity(current)
            or len(raw) != opened.st_size
            or not stat.S_ISREG(current.st_mode)
        ):
            raise ValueError("original support source changed while reading")
        return raw, (opened.st_dev, opened.st_ino)
    finally:
        os.close(fd)
