# Compatibility and proof limits

Python3.12+, Anthropic1.14.0, Pipedream2.1.20, FastAPI0.143.2, Uvicorn0.54.0, Pydantic2.14.1, httpx2 2.13.1and httpx0.28.1are pinned in the project/lock. Anthropic's actual public client requires httpx2objects; Pipedream's public async client uses old httpxobjects. Each owns a separate transport/client. SDK/provider/model substitutes and global/vendor patches are not part of the normal path.

## Actual baselines

Original3880a56passed7checks. Canonical original Anthropic summarize+Worker over real SDK/owned HTTP accepts max_tokens/null/missing/pause/tool text and records local delivery. Canonical original PipedreamActions+Worker over actual SDK/owned HTTP marks rejected/error/empty200as delivered; HTTP500produces three real actionPOSTs through default retries. Both are observed application/SDK behavior, with synthetic keys and no real Slack notification. The latter probe uses a declared synthetic-human summary seam and is not native model proof. Logs are recorded in PLAN.md.

## Source and native checkpoints

Human source40c76721b passed exact Linux38985414536; managed7259ba61d passed38986215797; owned local native916d41903 passed38987404300; required local native11690bccb4 passed38987791694; owned local webhook124a0d9088 passed38988875913. Current150includes both real SDKs, actual OAuth/action/served canonical CLI and required uncertainty/retained-effect fates. Exact current Linux, dataset and distribution gates remain required.

Managed source ABI2adds full destination/auth fingerprint and private live caller-authority binding. Existing incompatible files reject without repair; starter bytes remain unchanged. Complete earlier human source history is separate from current runtime operations/observations/decisions. Native/control observations cannot be imported through author fixtures. Current scoped reads, before/after source and real acknowledgements remain distinct from model prose.

Normal Pipedream uses explicit development/write/action gates, complete frozen target/auth settings, one OAuth plus one action request and zero retries/redirects. Original ret.ok must be exact true with the selected channel and message timestamp before the actual SDK consumes it. The [official Slack method](https://docs.slack.dev/reference/methods/chat.postMessage/) documents channel/message identifiers; account-specific component semantics and actual delivery are still unqualified. Reported remote_acknowledged is not promoted to verified external delivery. OAuth secrets are redacted/withheld with original hashes/provenance. Uncertainty is durably recorded before revocation when valid source permits; locks/corruption remain unconfirmed, and no uncertain notification is replayed.

The [official Anthropic SDK](https://github.com/anthropics/anthropic-sdk-python) and actual installed public types support owned clients/endpoints; the installed SDK's ambient header behavior is explicitly refused. The [Pipedream Connect documentation](https://pipedream.com/docs/connect/quickstart) requires project/account configuration. No source pin or synthetic surrounding enables a live model/account. Known original counters versus missing/invalid/zero and SDK normalization are recorded separately, with cost unknown.

## Independent completion gate

Twelve coherent private cases/init/inspect/separate preserving reset and fresh wheel/sdist/complete/full/production-only installations are next. Full source must rerun all checks, preserve every original byte/mode/hash manifest and reproduce portable archive bytes. Require exact pushed Linux success and equality with the downloaded complete source before independent completion. Enabled live provider/key/budget/semantic+billing, new Firedrill persisted cases, real support/Pipedream/Slack accounts and publication/license stay parked. The original compatibility seams do not qualify the normal owned runtime by themselves.

## Population and distribution implementation checkpoint

Owned dual-SDK15033773246ba231eca135e0d5127b25c1479ddd28e passed exact Linux38991642167. Twelve pinned populations preserve the original starter and complete human history; busy worlds have48tickets/72events/16notifications/96steps. Private controls bind exact original SQL cells/schema/UUID/header/fixture/reference and contain no imported scopes/reads/decisions. Installed immutable assets have a separate hardlink-tolerant bounded original-byte reader; mutable source aliases remain refused. Model-free separate reset preserves old files and effects. Version0.2.0packages these assets, commands, scripts, tests, lock and workflow. Fresh wheel/sdist/complete/full/production-only gates and downloaded exact Linux archive equality determine independent completion; their immutable run evidence is recorded separately in the catalog after successful execution.
