# Support escalation worker

A webhook worker reacts to synthetic ticket updates and explicit clock ticks. When an open ticket has an unanswered customer message older than two hours, the Anthropic Messages API drafts a brief internal escalation note. The worker rereads ticket state before delivery, so a late agent response or resolution arriving during drafting blocks the escalation. Event IDs and a durable outbox prevent repeated triggers from posting twice.

## Run locally

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```sh
uv sync --group dev
uv run support-escalation --seed
```

Set `EVENT_TOKEN`, `ANTHROPIC_API_KEY` and an enabled `ANTHROPIC_MODEL`, then start the local worker:

```sh
uv run support-escalation
```

Send a synthetic clock event with an explicit time:

```sh
curl -sS -X POST http://127.0.0.1:8767/events \
  -H 'content-type: application/json' -H 'x-event-token: local-secret' \
  --data '{"event_id":"EV-501","ticket_id":"T-501","kind":"clock_tick","occurred_at":"2026-10-15T12:30:00Z"}'
```

The fixture includes an unanswered open ticket, a resolved ticket, a ticket with an agent reply, and a recent request. The `clock_tick` event is the cooperative time seam: changing its timestamp does not run a background worker. `agent_reply`, `customer_reply` and `resolved` events update ticket state. The default `ACTION_MODE=local` writes to the SQLite outbox without calling a real service. Reset by creating a new database path; the seed command refuses to overwrite one.

## Pipedream Connect boundary

`ACTION_MODE=pipedream` plus `ALLOW_PIPEDREAM_ACTIONS=1` and all `PIPEDREAM_*` settings in `.env.example` selects the Pipedream development project action for a connected test Slack account. The adapter calls `slack_v2-post-message`; its account-specific authorization, channel configuration and returned payload still need live qualification. A failed or timed-out remote notification is marked `review_required` and is not automatically sent again. The app does not silently fall back to local delivery. The local action path is an application-side substitute, not interception of hosted Pipedream execution.

`uv run --group dev pytest` covers duplicate and conflicting IDs, explicit clock behavior, late replies, already resolved tickets, model failure retry before notification, remote uncertainty, token authentication and the Pipedream enablement gate. These are offline state and HTTP proofs. No live Claude, Pipedream, Slack or Firedrill call has been verified. This private repository has no license pending portfolio publication decisions.

Explore related applications in the [Firedrill example catalog](https://github.com/firedrill-tools/firedrill-example-catalog). Product and setup references: [Firedrill](https://firedrill.run/) and the [official developer documentation](https://docs.firedrill.run/). Use documentation matching the qualified release; this repository records its current integration limits above.
