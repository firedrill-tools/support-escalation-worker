# Scenarios

Seed a fresh database per independent case and inspect tickets and outbox rows.

- **Unanswered ticket:** T-501 receives a `clock_tick` at 12:30Z, more than two hours after its 10:00Z customer message. Queue and deliver one escalation.
- **Repeated trigger:** repeat the same event ID and then send another clock event. Neither creates a second notification.
- **Late response:** an `agent_reply` at 12:15Z arrives while a 12:30Z escalation summary is being drafted. The ticket revision changes and the escalation is blocked.
- **Already resolved:** T-502 or a ticket resolved during drafting must not notify.
- **Not due:** T-504 at 12:30Z is only one hour old. No model call or notification is needed.
- **Provider failure:** a model error before reserving notification can retry the same event safely.
- **Uncertain remote result:** a Pipedream timeout after notification starts requires manual review, not automatic duplicate posting.
