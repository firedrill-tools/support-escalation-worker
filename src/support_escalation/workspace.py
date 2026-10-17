"""Explicit synthetic-human authoring seam; not model read or write authority."""

from __future__ import annotations

import json
import os
import stat
from pathlib import Path
from uuid import uuid4

from .source import apply, parse
from .strict import MAX_BYTES, read_original


class Workspace:
    def __init__(self, path):
        supplied = Path(path).absolute()
        self.path = supplied.parent.resolve() / supplied.name
        self.closed = False
        self.damaged = False
        self.fd = None
        self.root_identity = self._physical(self.path.parent.stat())
        try:
            self.fd = os.open(self.path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            self.identity = self._physical(os.fstat(self.fd))
            self.read()
        except BaseException:
            self.close()
            raise

    @staticmethod
    def _physical(st):
        return st.st_dev, st.st_ino

    def _guard(self):
        if self.closed or self.damaged or self.fd is None:
            raise ValueError("support source owner is closed or damaged")
        st = self.path.lstat()
        if (
            not stat.S_ISREG(st.st_mode)
            or st.st_nlink != 1
            or self._physical(st) != self.identity
            or self._physical(os.fstat(self.fd)) != self.identity
            or self._physical(self.path.parent.stat()) != self.root_identity
        ):
            raise ValueError("original support source physical identity differs")

    def read(self):
        self._guard()
        raw, identity = read_original(self.path)
        self._guard()
        if identity != self.identity:
            raise ValueError("original support source changed during read")
        return raw, parse(raw)

    def inspect(self):
        return self.read()[1]

    def apply(self, action):
        original, _ = self.read()
        value, result = apply(original, action)
        if value is None:
            return result
        raw = (
            json.dumps(value, ensure_ascii=False, allow_nan=False, indent=2) + "\n"
        ).encode("utf-8", errors="strict")
        if len(raw) > MAX_BYTES:
            raise ValueError("complete support source exceeds4MiB")
        parse(raw)
        temporary = self.path.parent / (self.path.name + "." + str(uuid4()) + ".tmp")
        parent = None
        staged = None
        replaced = False
        try:
            fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            staged = self._physical(os.fstat(fd))
            try:
                with os.fdopen(os.dup(fd), "wb") as stream:
                    stream.write(raw)
                    stream.flush()
                os.fsync(fd)
                staged = self._physical(os.fstat(fd))
            finally:
                os.close(fd)
            current, _ = self.read()
            if current != original:
                raise ValueError("support source changed before commit")
            parent = os.open(self.path.parent, os.O_RDONLY)
            os.replace(temporary, self.path)
            replaced = True
            os.fsync(parent)
            fresh = os.open(self.path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            try:
                if self._physical(os.fstat(fresh)) != staged:
                    raise ValueError("new support source physical identity differs")
                old, self.fd = self.fd, fresh
                fresh = None
                self.identity = staged
                os.close(old)
            finally:
                if fresh is not None:
                    os.close(fresh)
            self._guard()
            return result
        except BaseException:
            if replaced:
                self.damaged = True
            raise
        finally:
            if parent is not None:
                os.close(parent)
            if temporary.exists() and self._physical(temporary.lstat()) == staged:
                temporary.unlink()

    def close(self):
        if self.closed:
            return
        self.closed = True
        if self.fd is not None:
            fd, self.fd = self.fd, None
            os.close(fd)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
