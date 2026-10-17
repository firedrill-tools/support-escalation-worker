"""Canonical owned serving and exclusive model-free managed initialization."""

import argparse
import json
import os
import sys
from pathlib import Path

from .managed import ManagedSource


def main():
    argv = sys.argv[1:]
    if "--seed" not in argv:
        from .managed_server import main as serve

        return serve(argv)
    parser = argparse.ArgumentParser(
        description="Initialize a new managed synthetic support database"
    )
    parser.add_argument("--seed", action="store_true", required=True)
    parser.add_argument(
        "--db", default=os.environ.get("ESCALATION_DB", "workspace/escalation.db")
    )
    parser.add_argument("--fixture")
    args = parser.parse_args(argv)
    fixture = (
        Path(args.fixture)
        if args.fixture
        else Path(__file__).parents[2] / "fixtures/starter.json"
    )
    source = ManagedSource.seed(args.db, fixture)
    try:
        result = {
            "database": str(source.path),
            "instance_id": source.instance_id,
            "source_version": 2,
            "initialized": True,
        }
    finally:
        cleanup = source.close()
        if not cleanup["database_closed"] or not cleanup["descriptor_closed"]:
            raise RuntimeError("support initialization cleanup is unconfirmed")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
