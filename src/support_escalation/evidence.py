"""Private per-event evidence. Source effects and recording success are separate."""

import json
import os
import stat
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from .strict import digest, sha256


def error_info(error):
    result = []
    seen = set()
    while error is not None and id(error) not in seen and len(result) < 8:
        seen.add(id(error))
        result.append({"type": type(error).__name__, "message": str(error)})
        error = error.__cause__
    return result


class Receipt:
    def __init__(self, root, initial):
        self.path = None
        self.error = None
        self.data = {
            "schemaVersion": 2,
            "run_id": str(uuid4()),
            "status": "prepared",
            "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": None,
            "source_images": {},
            "calls": [],
            "actions": [],
            "lifecycle": [],
            "acknowledged_decisions": [],
            "acknowledged_effects": [],
            "result": None,
            "error": None,
            "cleanup_confirmed": False,
            "recording_status": "durable",
            **deepcopy(initial),
        }
        try:
            root = Path(root).absolute()
            root.mkdir(parents=True, exist_ok=True, mode=0o700)
            st = root.lstat()
            if not stat.S_ISDIR(st.st_mode):
                raise ValueError("private nonsymlink evidence root required")
            directory = root / self.data["run_id"]
            directory.mkdir(mode=0o700)
            self.path = directory / "receipt.json"
            self.data["receipt_path"] = str(self.path)
            self.save()
        except BaseException as error:
            error.receipt = self
            raise

    def source(self, value):
        original = value["original"]
        if digest(original) != value["sha256"]:
            raise ValueError("original support source image hash differs")
        self.data["source_images"].setdefault(value["sha256"], deepcopy(original))
        return {"sha256": value["sha256"], "context_sha256": value["context_sha256"]}

    def save(self):
        if self.error is not None:
            raise self.error
        temporary = self.path.parent / ("receipt." + str(uuid4()) + ".tmp")
        fd = None
        parent = None
        identity = None
        try:
            raw = (
                json.dumps(self.data, ensure_ascii=False, allow_nan=False, indent=2)
                + "\n"
            ).encode("utf-8", errors="strict")
            if len(raw) > 128 * 1024 * 1024:
                raise ValueError("support receipt exceeds128MiB")
            fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            st = os.fstat(fd)
            identity = (st.st_dev, st.st_ino)
            with os.fdopen(os.dup(fd), "wb") as stream:
                stream.write(raw)
                stream.flush()
            os.fsync(fd)
            os.close(fd)
            fd = None
            parent = os.open(self.path.parent, os.O_RDONLY)
            os.replace(temporary, self.path)
            os.fsync(parent)
        except BaseException as error:
            self.error = error
            self.data["recording_status"] = "unknown"
            self.data["recording_error"] = error_info(error)
            raise
        finally:
            if fd is not None:
                os.close(fd)
            if parent is not None:
                os.close(parent)
            if temporary.exists():
                st = temporary.lstat()
                if (st.st_dev, st.st_ino) == identity:
                    temporary.unlink()
