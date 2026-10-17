# Support escalation worker

A webhook worker handles synthetic ticket updates and explicit clock ticks. An open ticket with an unanswered customer message at least two hours old can receive an Anthropic-generated internal escalation note. The application checks current committed state before queuing or dispatching. A late reply or resolution blocks an outdated draft. Repeated triggers never automatically resend an uncertain notification.

## Run

Requires Python3.12+ and [uv](https://docs.astral.sh/uv/).

```sh
uv sync --locked --group dev
uv run --locked support-escalation --seed --db workspace/escalation.db
# Configure EVENT_TOKEN, ANTHROPIC_API_KEY and an explicitly enabled ANTHROPIC_MODEL.
uv run --locked support-escalation --db workspace/escalation.db --allow-writes
```

Without `--allow-writes`, an eligible clock can produce a retained model preview without a business event or queue effect. Local success is `local_queued`; it does not claim external delivery. Initialization is model-free and exclusive, closes its database/descriptor before success and refuses to overwrite an existing path. Managed source format2adds complete destination and live caller-authority binding; older database formats reject without migration or repair. Preserve an old source/evidence and initialize a separate new path.

```sh
curl -sS -X POST http://127.0.0.1:8767/events \
  -H 'content-type: application/json' -H 'x-event-token: local-secret' \
  --data '{"event_id":"EV-501","ticket_id":"T-501","kind":"clock_tick","occurred_at":"2026-10-15T12:30:00Z"}'
```

Set EVENT_TOKEN to the header value used by your caller. Exact event fields are event_id, ticket_id, kind and occurred_at. Kinds are clock_tick, agent_reply, customer_reply and resolved. Timestamps are canonical UTC seconds. A clock is explicit application input; it does not advance wall time or start a background ticket timer. Subtraction handles the exact two-hour and final-calendar boundaries safely. A ticket has at most one escalation; later replies are retained without reopening its earlier notification. Event IDs cannot be reused with changed payloads.

## Pipedream development mode

```sh
uv run --locked support-escalation --db workspace/escalation.db --allow-writes \
  --action-mode pipedream --allow-pipedream-actions
```

Supply the development project, external user, connected Slack auth provision and explicit channel ID in `.env.example`. Choose either PIPEDREAM_ACCESS_TOKEN or the client ID/secret pair. The application snapshots these settings and binds their complete destination and credential fingerprint to the caller operation. Production environment settings, ambiguous credentials, missing permissions and malformed targets refuse before source/model allocation. ACTION_MODE and ALLOW_PIPEDREAM_ACTIONS remain recognized explicit server environment settings. Request JSON and model text cannot select the account, channel, endpoint, credentials or write permission.

Dispatch uses actual public AsyncPipedream2.1.20with its owned HTTPX0.28.1client, development mode, zero SDK/transport retries and no redirects. It allows one OAuth request and one action request. OAuth credential bodies are withheld from evidence; original hashes, explicitly redacted fields and unknown metadata remain. Known secrets never enter the model. The action is slack_v2-post-message. An original HTTP200return must contain exact boolean ret.ok=true, the selected channel and a message timestamp before SDK normalization. Remote success is `remote_acknowledged`, with actual delivery unknown. HTTP success alone, ret.false, empty/error data, a different channel or incomplete body cannot become delivery.

Cancellation or failure after reservation marks the existing notification review_required before scope revocation where current source permits. Corrupt source/locks remain unknown or unconfirmed. There is no automatic resend, local fallback or rollback claim after an earlier effect. Another event ID also cannot bypass an existing ticket escalation. Real account permissions, action-component behavior and Slack delivery still require separately authorized live qualification.

## Source and invocation boundaries

The normal run_event/canonical CLI/managed webhook share actual SQLite and native SDK ownership. The local-only run_local_event/support-escalation-local entry point cannot dispatch externally. The original injected Store/Worker/create_app and PipedreamActions functions remain explicit synthetic-human compatibility seams; normal serving no longer calls them.

```sh
uv run --locked support-escalation-state init --db workspace/other.db --fixture fixtures/starter.json
uv run --locked support-escalation-state inspect --db workspace/other.db
uv run --locked support-escalation-local --db workspace/other.db --allow-writes \
  --event-json '{"event_id":"EV-LOCAL","ticket_id":"T-501","kind":"clock_tick","occurred_at":"2026-10-15T12:30:00Z"}'
```

Private state commands are model-free and grant no runtime capability. Mutable source is bounded, regular, unshared and nonsymlink. Exact SQL schema/header/UUID/physical identity, original fixture, complete human history and current observation/decision replay are validated without repair. A live private credential also pins its original full caller intent, task, mode, destination and write permission. Source records cannot silently promote its authority. Each mutation needs a genuine own current unused read. Historical synthetic fixtures cannot import native/control observations or runtime capabilities. Current replies/resolution invalidate old draft reads.

The webhook verifies its caller token before body/source/model, accepts exact original duplicate-aware/fatalUTF8JSON, bounds body reading to64KiB/five seconds and limits admission to at most eight requests. Explicit incoming replies can commit through their separate current-intent boundary while a clock is drafting. Repeated incomplete state stays review_required; a durable active marker is not proof of live execution. GET/health reports admission and active requests without opening the source or calling a model.

## Evidence and cleanup

Private0700/0600receipts retain original SQL images, original model HTTP bytes/unknown fields, original versus SDK counters/normalization, actual source acknowledgements and original remote observations. Billing and actual external delivery remain unknown. A complete original Anthropic assistant end_turn/text/content/identity gate runs before SDK parsing or queuing. There is one prepared model request,256output tokens,2MiBrequest/1MiBresponse,30second HTTP phase and a shared10..300000millisecond deadline, with no retries or model fallback. Configure a model explicitly; a dependency pin does not enable one. ANTHROPIC_BASE_URL is not inherited, and ambient custom SDK headers are refused.

Caller/parent/deadline, direct task cancellation and actual CLI/server SIGINT/SIGTERM/SIGHUP revoke authority before cancelling/draining HTTP. Cleanup is shielded. Actual clients/transports/database/descriptors close once; a failed acknowledgement remains unconfirmed even if the resource closed. Earlier queues/remote acknowledgements survive later close or recording failure. Recording loss retains memory evidence and the last durable file, without recreating the missing receipt root. This is a cooperative application boundary, not an OS or distributed exactly-once guarantee.

## Private populations and preserving reset

```sh
uv run --locked support-escalation-cases list
uv run --locked support-escalation-cases init --case busy-due --root workspace/busy
uv run --locked support-escalation-cases inspect --root workspace/busy
uv run --locked support-escalation-cases reset --root workspace/busy --new-root workspace/busy-reset
```

There are three cases in each starter, busy, ambiguous and boundary profile. Busy histories contain48tickets,72prior events,16prior notifications and96explicit human steps, including eight local queues and eight uncertain remote reservations. Prior model execution, authentication and actual delivery remain unknown. The original starter is preserved byte for byte. Baselines may explicitly declare a safe initial revision; they cannot import an escalation or native authority.

Private case commands use no model or account and return no runtime capability. They bind the pinned case/fixture, original SQL baseline, source UUID/physical identity and private root/control. Reset exclusively creates a separate root with a fresh source UUID and seed, records its predecessor and preserves every old source effect and receipt. Existing targets, overlaps, aliases, copied/altered controls, imported capabilities and abandoned mutexes refuse without repair. Private expected outcomes and concurrent setup are never supplied to the model. Immutable installed assets may have installer hardlinks; mutable source and explicit fixture aliases remain refused.

## Verification and distributions

```sh
uv run --locked python scripts/build_populations.py --check
uv run --locked --group dev pytest -q
uv build
uv run --locked --group dev python scripts/check_distribution.py
```

The suite covers the original compatibility flows, strict source/history, managed SQL/current authority, actual Anthropic and Pipedream SDK/HTTP/OAuth, normal CLI/ASGI/served webhook, all signals/cancellation, current events, original response faults, uncertainty/no replay, failed closes, writer locks, recording loss after effects and all12private case/reset paths. Controlled HTTP uses synthetic keys and owned servers; it is not live model, Pipedream, Slack or Firedrill proof.

The distribution gate installs the wheel, source distribution and complete portable source outside the checkout using locked dependency hashes, with full development and production-only environments. Installed runs exercise all12cases, the real SDKs over owned HTTP, normal CLI/all three signals, missing credentials and an incomplete response after an earlier clock commit. Complete source reruns the whole suite. The portable source archive records every original file byte, mode and hash, is reproduced repeatedly, and is compared with the exact successful Linux download before independent completion is recorded in the catalog.

Live provider/account/budget/semantic and billing review, new Firedrill qualification and publication/license remain parked. This private repository has no selected public license.

See the [example catalog](https://github.com/firedrill-tools/firedrill-example-catalog), [Firedrill](https://firedrill.run/) and its [current official documentation](https://docs.firedrill.run/). Version/proof details are in [PLAN.md](PLAN.md) and [COMPATIBILITY.md](COMPATIBILITY.md).
