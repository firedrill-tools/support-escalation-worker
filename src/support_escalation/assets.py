"""Pinned read-only package data may be hard-linked by an installer."""

import os
import stat
from pathlib import Path

from ._dataset_manifest import (
    DATASET_BYTES,
    DATASET_SHA256,
    STARTER_BYTES,
    STARTER_SHA256,
)
from .strict import MAX_BYTES, sha256

DATA_ROOT = Path(__file__).parent / "data"
MANIFEST = {
    "starter.json": (STARTER_BYTES, STARTER_SHA256),
    "populations.json": (DATASET_BYTES, DATASET_SHA256),
}


def read_asset(name):
    if name not in MANIFEST:
        raise ValueError("unknown pinned support package asset")
    expected_bytes, expected_sha = MANIFEST[name]
    path = DATA_ROOT / name
    before = path.lstat()
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_nlink < 1
        or not 0 < before.st_size <= MAX_BYTES
    ):
        raise ValueError("bounded regular nonsymlink package asset required")
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        opened = os.fstat(fd)
        with os.fdopen(os.dup(fd), "rb") as source:
            raw = source.read(MAX_BYTES + 1)
        after, current = os.fstat(fd), path.lstat()

        def identity(value):
            return (
                value.st_dev,
                value.st_ino,
                value.st_size,
                value.st_nlink,
                value.st_mtime_ns,
                value.st_ctime_ns,
            )

        if (
            identity(before) != identity(opened)
            or identity(opened) != identity(after)
            or identity(after) != identity(current)
            or not stat.S_ISREG(current.st_mode)
            or len(raw) != expected_bytes
            or sha256(raw) != expected_sha
        ):
            raise ValueError("original pinned package asset bytes/identity/hash differ")
        return raw
    finally:
        os.close(fd)
