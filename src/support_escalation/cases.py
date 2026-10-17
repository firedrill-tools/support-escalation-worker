"""Private model-free case initialization, inspection and separate preserving reset."""

import argparse
import base64
import json
import os
import sqlite3
import stat
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from . import managed_state as state
from .managed import ManagedSource
from .populations import checked, dataset, definition, fixture_bytes, reference, sealed
from .source import PROVENANCE
from .strict import canonical, digest, keys, parse_json, read_original


def root_path(path):
    supplied = Path(path).absolute()
    if not stat.S_ISDIR(supplied.lstat().st_mode):
        raise ValueError("original regular nonsymlink support case root required")
    return supplied.resolve()


@contextmanager
def control_lock(root):
    path = root / ".case-control.lock"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    identity = os.fstat(fd)
    try:
        os.write(fd, str(uuid4()).encode())
        os.fsync(fd)
        yield
    finally:
        os.close(fd)
        current = path.lstat()
        if (current.st_dev, current.st_ino) != (identity.st_dev, identity.st_ino):
            raise ValueError("private support case mutex identity changed")
        path.unlink()


def write_exclusive(path, raw):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as output:
        output.write(raw)
        output.flush()
        os.fsync(output.fileno())


def close_source(source):
    cleanup = source.close()
    if not cleanup["database_closed"] or not cleanup["descriptor_closed"]:
        raise RuntimeError("private support source cleanup unconfirmed")


def initialize(root, case_id, *, generation=1, reset_of=None):
    case = definition(case_id)
    supplied = Path(root).absolute()
    supplied.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    root = supplied.parent.resolve() / supplied.name
    root.mkdir(mode=0o700)  # Exclusive; preserve partial initialization on failure.
    write_exclusive(root / "fixture.json", fixture_bytes(case))
    source = ManagedSource.seed(root / "support.db", root / "fixture.json")
    try:
        before = source.snapshot()
        ref = reference(case)
        seed = sealed(
            {
                "schemaVersion": 1,
                "instance_id": source.instance_id,
                "reference": ref,
                "original": before["original"],
                "original_sha256": before["sha256"],
                "context_sha256": before["context_sha256"],
                "provenance": PROVENANCE,
            }
        )

        def publish():
            current = state.snapshot(
                source.database, source.identity, source.instance_id
            )
            if current["sha256"] != before["sha256"]:
                raise ValueError(
                    "original support source changed during initialization"
                )
            header = {**before["source"], "creation": "population", "population": ref}
            source.database.execute(
                "UPDATE metadata SET document=? WHERE id=1", (canonical(header),)
            )
            source.database.execute(
                "INSERT INTO seed_state VALUES (1,?)", (canonical(seed),)
            )
            state.snapshot(source.database, source.identity, source.instance_id)

        source._transaction(publish)
    finally:
        close_source(source)
    # Reopen only after the original fixture owner has closed; its old header pin
    # is never changed to authorize mutation in the normal runtime.
    source = ManagedSource(root / "support.db")
    try:
        snapshot = source.snapshot()
        control = sealed(
            {
                "schemaVersion": 1,
                "kind": "private_synthetic_support_case",
                "root": str(root),
                "root_identity": source._physical(root.stat()),
                "database": "support.db",
                "instance_id": source.instance_id,
                "reference": ref,
                "seed_sha256": seed["content_sha256"],
                "header_sha256": digest(snapshot["source"]),
                "generation": generation,
                "reset_of": reset_of,
            }
        )
    finally:
        close_source(source)
    write_exclusive(root / "control.json", (canonical(control) + "\n").encode())
    parent = os.open(root, os.O_RDONLY)
    try:
        os.fsync(parent)
    finally:
        os.close(parent)
    return inspect_case(root)


def _inspect(root):
    control = checked(parse_json(read_original(root / "control.json")[0]))
    keys(
        control,
        {
            "schemaVersion",
            "kind",
            "root",
            "root_identity",
            "database",
            "instance_id",
            "reference",
            "seed_sha256",
            "header_sha256",
            "generation",
            "reset_of",
            "content_sha256",
        },
    )
    if (
        type(control["schemaVersion"]) is not int
        or control["schemaVersion"] != 1
        or control["kind"] != "private_synthetic_support_case"
        or control["root"] != str(root)
        or control["root_identity"] != ManagedSource._physical(root.stat())
        or control["database"] != "support.db"
        or type(control["generation"]) is not int
        or not 1 <= control["generation"] <= 2**31 - 1
    ):
        raise ValueError("original private support control identity differs")
    state.uuid(control["instance_id"])
    state.hex_digest(control["seed_sha256"])
    state.hex_digest(control["header_sha256"])
    if control["reset_of"] is not None:
        previous = keys(control["reset_of"], {"root", "instance_id", "source_sha256"})
        if (
            type(previous["root"]) is not str
            or not previous["root"]
            or not Path(previous["root"]).is_absolute()
            or control["generation"] == 1
        ):
            raise ValueError("original reset predecessor required")
        state.uuid(previous["instance_id"])
        state.hex_digest(previous["source_sha256"])
    elif control["generation"] != 1:
        raise ValueError("reset generation has no predecessor")
    case = definition(control["reference"]["case_id"])
    source = ManagedSource(root / "support.db")
    try:
        snapshot = source.snapshot()
        raw = snapshot["original"]["tables"]["seed_state"]
        if len(raw) != 1:
            raise ValueError("original population source control missing")
        seed = parse_json(base64.b64decode(raw[0]["document_base64"], validate=True))
        if (
            control["instance_id"] != source.instance_id
            or control["reference"] != reference(case)
            or snapshot["source"]["population"] != control["reference"]
            or control["seed_sha256"] != seed["content_sha256"]
            or control["header_sha256"] != digest(snapshot["source"])
            or read_original(root / "fixture.json")[0] != fixture_bytes(case)
        ):
            raise ValueError("original private support control/source/seed differs")
        return {
            "root": str(root),
            "database": str(source.path),
            "instance_id": source.instance_id,
            "case_id": case["id"],
            "profile": case["profile"],
            "title": case["title"],
            "generation": control["generation"],
            "reset_of": control["reset_of"],
            "events": case["events"],
            "mode": case["mode"],
            "private_during": case["during"],
            "private_after": case["after"],
            "private_reference": case["reference"],
            "population": control["reference"],
            "seed_sha256": seed["content_sha256"],
            "source_sha256": snapshot["sha256"],
            "metrics": {
                key: len(snapshot["business"][key])
                for key in ("tickets", "events", "outbox")
            },
            "source": snapshot,
            "delivery": {
                "actual_delivery": None,
                "prior_model": None,
                "authentication": None,
            },
            "authority": "private model-free inspection; no runtime read or write capability",
        }
    finally:
        close_source(source)


def inspect_case(root):
    root = root_path(root)
    with control_lock(root):
        return _inspect(root)


def reset_case(root, new_root):
    root = root_path(root)
    target = Path(new_root).absolute()
    candidate = target.parent.resolve() / target.name
    if candidate.is_relative_to(root) or root.is_relative_to(candidate):
        raise ValueError(
            "reset requires a separate new root; preserve old source and evidence"
        )
    with control_lock(root):
        previous = _inspect(root)
        if previous["generation"] == 2**31 - 1:
            raise ValueError("private reset generation exhausted")
        return initialize(
            candidate,
            previous["case_id"],
            generation=previous["generation"] + 1,
            reset_of={
                "root": str(root),
                "instance_id": previous["instance_id"],
                "source_sha256": previous["source_sha256"],
            },
        )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list")
    init = commands.add_parser("init")
    init.add_argument("--case", required=True)
    init.add_argument("--root", required=True)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--root", required=True)
    reset = commands.add_parser("reset")
    reset.add_argument("--root", required=True)
    reset.add_argument("--new-root", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "list":
            data = dataset()
            result = {
                "id": data["id"],
                "version": data["version"],
                "cases": [
                    {key: c[key] for key in ("id", "profile", "title")}
                    for c in data["cases"]
                ],
            }
        elif args.command == "init":
            result = initialize(args.root, args.case)
        elif args.command == "inspect":
            result = inspect_case(args.root)
        else:
            result = reset_case(args.root, args.new_root)
        print(json.dumps(result, ensure_ascii=False, allow_nan=False, indent=2))
    except (OSError, ValueError, KeyError, sqlite3.Error) as error:
        print(json.dumps({"error": str(error)}))
        raise SystemExit(2) from error


if __name__ == "__main__":
    main()
