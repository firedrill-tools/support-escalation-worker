"""Synthetic local delivery or explicit Pipedream Connect action."""

import os
from pipedream import Pipedream


class LocalActions:
    def notify(self, event_id: str, ticket_id: str, summary: str):
        return {"event_id": event_id, "ticket_id": ticket_id, "local_outbox": True}


class PipedreamActions:
    def __init__(self):
        required = ["PIPEDREAM_PROJECT_ID", "PIPEDREAM_CLIENT_ID", "PIPEDREAM_CLIENT_SECRET",
                    "PIPEDREAM_EXTERNAL_USER_ID", "PIPEDREAM_SLACK_AUTH_PROVISION_ID",
                    "PIPEDREAM_SLACK_CHANNEL"]
        missing = [name for name in required if not os.getenv(name)]
        if missing or os.getenv("ALLOW_PIPEDREAM_ACTIONS") != "1":
            raise RuntimeError("Pipedream development configuration and ALLOW_PIPEDREAM_ACTIONS=1 required: " + ", ".join(missing))
        self.client = Pipedream(client_id=os.environ["PIPEDREAM_CLIENT_ID"],
                                client_secret=os.environ["PIPEDREAM_CLIENT_SECRET"],
                                project_id=os.environ["PIPEDREAM_PROJECT_ID"],
                                project_environment="development")

    def notify(self, event_id: str, ticket_id: str, summary: str):
        return self.client.actions.run(
            id="slack_v2-post-message", external_user_id=os.environ["PIPEDREAM_EXTERNAL_USER_ID"],
            configured_props={
                "slack_v2": {"authProvisionId": os.environ["PIPEDREAM_SLACK_AUTH_PROVISION_ID"]},
                "channel": os.environ["PIPEDREAM_SLACK_CHANNEL"],
                "text": f"Support escalation {ticket_id} ({event_id}): {summary}",
            },
        )
