"""Real private SQL/files/CLI/HTTP populations; no model or SDK substitute."""

import base64
import json
import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from support_escalation import assets
from support_escalation.cases import initialize, inspect_case, reset_case
from support_escalation.domain import initial
from support_escalation.managed import ManagedSource
from support_escalation.populations import dataset, definition, fixture_bytes, sealed
from support_escalation.strict import MAX_INTEGER, canonical, digest, sha256

ROOT = Path(__file__).parents[1]


def test_all_twelve_normal_installed_cli_sdk_http_populations_and_preserving_resets(
    tmp_path,
):
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            str(ROOT / "scripts/qualify_cases.py"),
            "--root",
            str(tmp_path / "runs"),
            "--report",
            str(tmp_path / "report.json"),
        ],
        capture_output=True,
        text=True,
        timeout=240,
        cwd=tmp_path,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads((tmp_path / "report.json").read_text())
    assert report["count"] == 12
    assert sum(c["model_requests"] for c in report["cases"]) == 10
    assert sum(c["action_requests"] for c in report["cases"]) == 1


def test_authored_busy_source_complete_history_and_private_copies():
    data = dataset()
    assert len(data["cases"]) == 12
    assert {
        p: sum(c["profile"] == p for c in data["cases"])
        for p in ("starter", "busy", "ambiguous", "boundary")
    } == dict.fromkeys(("starter", "busy", "ambiguous", "boundary"), 3)
    case = definition("busy-due")
    assert case["reference"]["prior_human_steps"] == 96
    case["events"][0]["ticket_id"] = "T-FOREIGN"
    assert definition("busy-due")["events"][0]["ticket_id"] == "T-601"
    assert (
        assets.read_asset("starter.json")
        == (ROOT / "fixtures/starter.json").read_bytes()
    )


@pytest.mark.parametrize("revision", [0, -1, True, 1.0, MAX_INTEGER + 1])
def test_declared_base_revision_requires_original_exact_safe_value(revision):
    value = json.loads(assets.read_asset("starter.json"))
    value["baselineVersion"] = 1
    for row in value["tickets"]:
        row["revision"] = 1
    value["tickets"][0]["revision"] = revision
    with pytest.raises(ValueError):
        initial(value)


def test_installer_asset_hardlink_allowed_but_source_alias_refused(
    tmp_path, monkeypatch
):
    data = tmp_path / "data"
    data.mkdir()
    original = tmp_path / "original.json"
    original.write_bytes(assets.read_asset("starter.json"))
    os.link(original, data / "starter.json")
    monkeypatch.setattr(assets, "DATA_ROOT", data)
    assert assets.read_asset("starter.json") == original.read_bytes()
    with pytest.raises(ValueError):
        ManagedSource.seed(tmp_path / "support.db", original)
    assert not (tmp_path / "support.db").exists()


@pytest.mark.parametrize("damage", ["symlink", "byte", "truncated"])
def test_pinned_asset_original_alias_or_bytes_rejected(tmp_path, monkeypatch, damage):
    raw = assets.read_asset("starter.json")
    source = tmp_path / "source"
    source.write_bytes(raw)
    data = tmp_path / "data"
    data.mkdir()
    target = data / "starter.json"
    if damage == "symlink":
        target.symlink_to(source)
    else:
        target.write_bytes(
            raw[:-1] if damage == "truncated" else raw.replace(b"Maya", b"Mira")
        )
    monkeypatch.setattr(assets, "DATA_ROOT", data)
    with pytest.raises(ValueError):
        assets.read_asset("starter.json")


@pytest.mark.parametrize(
    "damage",
    [
        "missing_seed",
        "rehashed_business",
        "rehashed_header",
        "imported_authority",
        "rehashed_reference",
        "stripped_population",
    ],
)
def test_original_population_seed_and_control_cannot_be_fabricated_by_rehashing(
    tmp_path, damage
):
    root = tmp_path / "case"
    initialize(root, "starter-due")
    with sqlite3.connect(root / "support.db") as db:
        seed = json.loads(db.execute("SELECT document FROM seed_state").fetchone()[0])
        header = json.loads(db.execute("SELECT document FROM metadata").fetchone()[0])
        if damage == "missing_seed":
            db.execute("DELETE FROM seed_state")
        elif damage == "stripped_population":
            header.update(creation="fixture", population=None)
            db.execute("UPDATE metadata SET document=?", (canonical(header),))
            db.execute("DELETE FROM seed_state")
        else:
            if damage in ("rehashed_business", "rehashed_header"):
                table = "business" if damage == "rehashed_business" else "metadata"
                row = seed["original"]["tables"][table][0]
                value = json.loads(base64.b64decode(row["document_base64"]))
                if table == "business":
                    value["tickets"][0]["customer"] = "Changed Customer"
                else:
                    value["physical_identity"] = "0:999"
                raw = canonical(value).encode()
                row.update(
                    document_base64=base64.b64encode(raw).decode(),
                    document_sha256=sha256(raw),
                )
            elif damage == "imported_authority":
                seed["original"]["tables"]["operations"] = [
                    {
                        "id": "fabricated",
                        "document_base64": "e30=",
                        "document_sha256": sha256(b"{}"),
                    }
                ]
            else:
                seed["reference"]["definition_sha256"] = "0" * 64
            seed["original_sha256"] = digest(seed["original"])
            seed = sealed({k: v for k, v in seed.items() if k != "content_sha256"})
            db.execute("UPDATE seed_state SET document=?", (canonical(seed),))
    before = (root / "support.db").read_bytes()
    with pytest.raises(ValueError):
        inspect_case(root)
    assert (root / "support.db").read_bytes() == before
    assert not (root / ".case-control.lock").exists()


def test_copy_alias_foreign_rehashed_control_and_stale_mutex_reject_without_repair(
    tmp_path,
):
    original = tmp_path / "original"
    initialize(original, "starter-due")
    copied = tmp_path / "copied"
    shutil.copytree(original, copied)
    with pytest.raises(ValueError):
        inspect_case(copied)
    alias = tmp_path / "alias"
    alias.symlink_to(original, target_is_directory=True)
    with pytest.raises(ValueError):
        inspect_case(alias)
    control = json.loads((original / "control.json").read_text())
    control["reference"]["case_id"] = "starter-recent"
    control = sealed({k: v for k, v in control.items() if k != "content_sha256"})
    (original / "control.json").write_text(canonical(control))
    with pytest.raises(ValueError):
        inspect_case(original)
    mutex = original / ".case-control.lock"
    mutex.write_text("old owner is unknown")
    with pytest.raises(FileExistsError):
        inspect_case(original)
    assert mutex.read_text() == "old owner is unknown"


def test_exclusive_init_reset_overlap_collision_and_preserve_after_human_effect(
    tmp_path,
):
    root = tmp_path / "case"
    initial = initialize(root, "starter-due")
    with ManagedSource(initial["database"]) as source:
        source.event(
            {
                "event_id": "EV-HUMAN",
                "ticket_id": "T-501",
                "kind": "agent_reply",
                "occurred_at": "2026-10-15T12:31:00Z",
            }
        )
    before = (root / "support.db").read_bytes()
    with pytest.raises(FileExistsError):
        initialize(root, "starter-due")
    for target in (root, root / "child", tmp_path):
        with pytest.raises(ValueError):
            reset_case(root, target)
    collision = tmp_path / "existing"
    collision.mkdir()
    (collision / "keep").write_text("preserve")
    with pytest.raises(FileExistsError):
        reset_case(root, collision)
    assert (collision / "keep").read_text() == "preserve"
    assert (root / "support.db").read_bytes() == before
    assert root.stat().st_mode & 0o777 == 0o700
    assert (root / "control.json").stat().st_mode & 0o777 == 0o600
    new = reset_case(root, tmp_path / "new")
    assert new["generation"] == 2
    assert new["instance_id"] != initial["instance_id"]
    assert new["source"]["business"] == initial["source"]["business"]
    assert (root / "support.db").read_bytes() == before
