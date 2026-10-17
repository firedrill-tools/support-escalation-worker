"""Private model-free managed source commands; never returns runtime capabilities."""

import argparse
import json

from .managed import ManagedSource
from .strict import parse_json


def main():
    parser = argparse.ArgumentParser(
        description="Private support source init/inspect/event"
    )
    sub = parser.add_subparsers(dest="command", required=True)
    init = sub.add_parser("init")
    init.add_argument("--db", required=True)
    init.add_argument("--fixture", required=True)
    inspect = sub.add_parser("inspect")
    inspect.add_argument("--db", required=True)
    event = sub.add_parser("event")
    event.add_argument("--db", required=True)
    event.add_argument("--event-json", required=True)
    args = parser.parse_args()
    try:
        source = (
            ManagedSource.seed(args.db, args.fixture)
            if args.command == "init"
            else ManagedSource(args.db)
        )
        try:
            result = (
                source.event(
                    parse_json(args.event_json.encode("utf-8", errors="strict"))
                )
                if args.command == "event"
                else source.snapshot()
            )
        finally:
            cleanup = source.close()
            if not cleanup["database_closed"] or not cleanup["descriptor_closed"]:
                raise RuntimeError("private support source cleanup is unconfirmed")
        print(json.dumps(result, ensure_ascii=False, allow_nan=False, indent=2))
    except Exception as error:
        print(json.dumps({"error": str(error)}))
        raise SystemExit(2) from error


if __name__ == "__main__":
    main()
