import argparse
import os
from pathlib import Path
import uvicorn

from .actions import LocalActions, PipedreamActions
from .api import create_app
from .model import summarize
from .service import Store, Worker


def main():
    parser = argparse.ArgumentParser(description="Synthetic support escalation webhook")
    parser.add_argument("--seed", action="store_true")
    parser.add_argument("--db", default=os.getenv("ESCALATION_DB", "workspace/escalation.db"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8767)
    args = parser.parse_args()
    if args.seed:
        print(Store.seed(args.db, Path(__file__).parents[2] / "fixtures" / "starter.json").path)
        return
    mode = os.getenv("ACTION_MODE", "local")
    if mode not in ("local", "pipedream"):
        parser.error("ACTION_MODE must be local or pipedream")
    actions = LocalActions() if mode == "local" else PipedreamActions()
    app = create_app(Worker(Store(args.db), summarize, actions), os.getenv("EVENT_TOKEN", ""))
    uvicorn.run(app, host=args.host, port=args.port)
