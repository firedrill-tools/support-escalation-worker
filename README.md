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

## Owned local event runtime

The new managed local entry point uses actual Anthropic1.14.0with its public httpx2client, one owned native transport and the managed current-source boundary. The original injected Store/Worker/API functions remain trusted synthetic-human compatibility seams. The original serving command and Pipedream path are still being integrated into the owned lifecycle; they are not qualified by the new local checks.

```sh
uv run --locked support-escalation-state init --db workspace/managed.db --fixture fixtures/starter.json
# Set ANTHROPIC_API_KEY and an explicitly enabled ANTHROPIC_MODEL first.
uv run --locked support-escalation-local --db workspace/managed.db --allow-writes \
  --event-json '{"event_id":"EV-OWNED-1","ticket_id":"T-501","kind":"clock_tick","occurred_at":"2026-10-15T12:30:00Z"}'
```

Without `--allow-writes`, an eligible event can produce a retained model preview with no business event or queue effect. Local success means `local_queued`, with external delivery unknown. Repeated recorded events are inspected without replaying the model or notification. A prior incomplete clock or started remote notification returns review_required. Late replies/resolution invalidate old draft reads; current reinspection blocks the old queue.

Private0700/0600receipts are prepared before keys/resources. They retain original SQL images, complete original request/response bytes/unknown fields, exact original versus SDK completion/usage, real read/decision acknowledgements and final valid/unknown business. An explicit original assistant end_turn with bounded nonempty text must pass before SDK parsing and queuing. max_tokens/null/missing/pause_turn/tool_use refuse before the queue while preserving the earlier recorded clock. One prepared native request,256output tokens,2MiBrequest/1MiBresponse,30second HTTP phase, shared bounded deadline and zero retries are explicit. No default live model is selected; an explicit base origin is supported, and ambient ANTHROPIC_BASE_URL is not inherited. Ambient custom SDK headers are refused.

Caller/parent/deadline, direct task cancellation and actual SIGINT/SIGTERM/SIGHUP close the durable scope before cancelling/draining owned HTTP. Final cleanup is shielded and closes provider/database/descriptor once. Actual control failure remains unconfirmed; earlier source effects are retained. All116current checks pass, including44normal actual local SDK/HTTP/CLI/lifecycle checks. Original malformed/partial/status/usage, current source damage, recording loss, failed one-attempt closes and lock after the queue are exercised without automatic repair or retry. The owned webhook/Pipedream route,12coherent private populations and fresh full/production package qualification remain active. Controlled local HTTP is not live Claude, Pipedream, Slack or Firedrill proof.
