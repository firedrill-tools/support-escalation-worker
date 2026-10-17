"""Actual Uvicorn serving; stop admission/events before draining on all three signals."""

import argparse
import contextlib
import os
import signal
import threading

import uvicorn

from .managed_api import create_managed_app


class OwnedServer(uvicorn.Server):
    def __init__(self, config, admission):
        super().__init__(config)
        self.admission = admission

    def handle_exit(self, sig, frame):
        self.admission.stop(signal.Signals(sig).name)
        self.should_exit = True

    @contextlib.contextmanager
    def capture_signals(self):
        if threading.current_thread() is not threading.main_thread():
            yield
            return
        previous = {
            s: signal.signal(s, self.handle_exit)
            for s in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
        }
        try:
            yield
        finally:
            for s, handler in previous.items():
                signal.signal(s, handler)


def main():
    parser = argparse.ArgumentParser(
        description="Owned local support escalation webhook"
    )
    parser.add_argument("--db", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8767)
    parser.add_argument("--allow-writes", action="store_true")
    parser.add_argument("--model")
    parser.add_argument("--base-url")
    parser.add_argument("--output", default="artifacts/support-escalation")
    parser.add_argument("--timeout-ms", type=int, default=120000)
    args = parser.parse_args()
    app = create_managed_app(
        database=args.db,
        event_token=os.environ.get("EVENT_TOKEN", ""),
        allow_writes=args.allow_writes,
        model=args.model,
        base_url=args.base_url,
        output_root=args.output,
        timeout_ms=args.timeout_ms,
    )
    server = OwnedServer(
        uvicorn.Config(
            app,
            host=args.host,
            port=args.port,
            access_log=False,
            proxy_headers=False,
            timeout_keep_alive=5,
            timeout_graceful_shutdown=10,
        ),
        app.state.support_admission,
    )
    server.run()


if __name__ == "__main__":
    main()
