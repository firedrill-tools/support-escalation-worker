"""Owned normal event workflow: local queue or explicitly authorized Pipedream."""

from __future__ import annotations

import asyncio
import time
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from .domain import OWNED_OBSERVATION, event
from .delivery import Delivery, config as delivery_config
from .evidence import Receipt, error_info
from .managed import ManagedSource
from .native import Native, config
from .strict import canonical, parse_json

TASK = "Evaluate this exact caller ticket event and prepare a needed internal escalation through the selected authorized mode."


class Run:
    def __init__(self, receipt, timeout_ms):
        self.receipt = receipt
        self.deadline = time.monotonic() + timeout_ms / 1000
        self.source = None
        self.scope = None
        self.native = None
        self.delivery = None
        self.delivery_read = None
        self.mode = "local"
        self.job = None
        self.failure = None
        self.closing = False
        self.scope_close_attempted = False
        self.scope_closed = False
        self.flow_drained = True
        self.source_attempted = False
        self.stop_events = []

    def fail(self, error):
        if self.failure is None:
            self.failure = error

    def save(self):
        try:
            self.receipt.save()
        except BaseException as error:
            self.fail(error)
            raise

    def basic(self):
        if self.failure is not None:
            raise self.failure
        if self.closing:
            raise RuntimeError("support operation is closing")
        if any(event.is_set() for event in self.stop_events):
            raise RuntimeError("support cancellation requested")
        if time.monotonic() >= self.deadline:
            raise TimeoutError("support shared deadline exceeded")
        if self.receipt.error is not None:
            raise self.receipt.error

    def check(self):
        self.basic()
        if self.source is not None and self.scope is not None:
            self.source._operation(self.source.snapshot(), self.scope)

    def close_scope(self):
        if self.scope_close_attempted:
            return
        if self.scope is None:
            self.scope_closed = True
            return
        self.scope_close_attempted = True
        if self.mode == "pipedream":
            try:
                uncertain = self.source.mark_uncertain(self.scope)
                if uncertain is not None:
                    self.receipt.data["acknowledged_decisions"].append(
                        deepcopy(uncertain)
                    )
                    if uncertain["decision"]["status"] == "applied":
                        self.receipt.data["acknowledged_effects"].append(
                            deepcopy(uncertain)
                        )
                    self.receipt.data["actions"].append(
                        {
                            "kind": "owned_uncertainty_before_revocation",
                            "result": deepcopy(uncertain),
                            "source": self.receipt.source(self.source.snapshot()),
                        }
                    )
                    if self.receipt.error is None:
                        self.save()
            except BaseException as error:
                self.receipt.data["uncertainty_record_error"] = error_info(error)
                self.fail(error)
        try:
            result = self.source.close_operation(self.scope)
            self.scope_closed = result["closed"]
            self.receipt.data["scope_close"] = result
            self.receipt.data["lifecycle"].append(
                {"kind": "scope_close_returned", "closed": self.scope_closed}
            )
        except BaseException as error:
            self.receipt.data["scope_close_error"] = error_info(error)
            self.fail(error)

    def cancel(self, error):
        if self.closing:
            self.fail(error)
            return
        self.closing = True
        self.fail(error)
        self.close_scope()
        if self.job is not None and not self.job.done():
            self.receipt.data["lifecycle"].append(
                {"kind": "flow_cancel_requested", "scope_closed": self.scope_closed}
            )
            self.job.cancel()

    def observe(self):
        self.check()
        read = self.source.observe(self.scope)
        self.receipt.data["actions"].append(
            {
                "kind": "own_current_read",
                "observation": deepcopy(read["observation"]),
                "source": self.receipt.source(self.source.snapshot()),
            }
        )
        self.save()
        return read

    def delivery_check(self, delivery):
        self.check()
        if (
            self.mode != "pipedream"
            or self.delivery is not delivery
            or self.delivery_read is None
        ):
            raise ValueError("owned current Pipedream reservation required")
        source = self.source.snapshot()
        operation = self.source._operation(source, self.scope)
        target = delivery.config.destination()
        pending = next(
            (
                r
                for r in source["business"]["outbox"]
                if r["event_id"] == operation["intent"]["event"]["event_id"]
            ),
            None,
        )
        if (
            operation["destination"] != target
            or pending is None
            or pending["destination"] != target
            or pending["status"] != "notification_started"
            or source["context_sha256"]
            != self.delivery_read["observation"]["context_sha256"]
        ):
            raise ValueError("current source/destination changed before notification")
        if self.delivery_read["value"]["ticket"] != next(
            r for r in source["business"]["tickets"] if r["id"] == pending["ticket_id"]
        ):
            raise ValueError("selected ticket changed before notification")

    def apply(self, action, read):
        self.check()
        result = self.source.commit(
            self.scope, action, read["observation"]["id"], read["proof"]
        )
        self.receipt.data["acknowledged_decisions"].append(deepcopy(result))
        if result["decision"]["status"] == "applied":
            self.receipt.data["acknowledged_effects"].append(deepcopy(result))
        self.receipt.data["actions"].append(
            {
                "kind": "source_decision",
                "result": deepcopy(result),
                "source": self.receipt.source(self.source.snapshot()),
            }
        )
        self.save()
        return result["result"]


def observed(source, payload):
    row = next(
        r for r in source["business"]["tickets"] if r["id"] == payload["ticket_id"]
    )
    entry = next(
        (
            r
            for r in source["business"]["events"]
            if r["payload"]["event_id"] == payload["event_id"]
        ),
        None,
    )
    outbox = next(
        (
            r
            for r in source["business"]["outbox"]
            if r["event_id"] == payload["event_id"]
        ),
        None,
    )
    return {
        "status": "valid",
        "as_observed_context_sha256": source["context_sha256"],
        "ticket": deepcopy(row),
        "event": deepcopy(entry),
        "outbox": deepcopy(outbox),
        "local_queue_complete": outbox is not None
        and outbox["status"] == "local_queued",
        "notification_state": outbox["status"] if outbox is not None else None,
        "workflow_complete": outbox is not None
        and outbox["status"] in ("local_queued", "remote_acknowledged"),
        "actual_external_delivery": None,
    }


async def run_event(**options):
    """Actual owned event execution; external dispatch requires separate explicit gates."""
    allowed = {
        "database",
        "event",
        "intent",
        "allow_writes",
        "api_key",
        "model",
        "base_url",
        "output_root",
        "timeout_ms",
        "stop_event",
        "parent_event",
        "action_mode",
        "pipedream",
        "allow_pipedream_actions",
    }
    payload = deepcopy(options.get("event"))
    intent = deepcopy(options.get("intent"))
    writes = options.get("allow_writes", False)
    api_key = options.get("api_key")
    model = options.get("model")
    base_url = options.get("base_url")
    database = options.get("database")
    mode = options.get("action_mode", "local")
    pipe_settings = deepcopy(options.get("pipedream"))
    pipe_allowed = options.get("allow_pipedream_actions", False)
    timeout_ms = options.get("timeout_ms", 120000)
    signals = [options.get("stop_event"), options.get("parent_event")]
    receipt = None
    owner = None
    watchers = []
    deadline_timer = None
    outcome = None
    summary = None
    try:
        receipt = Receipt(
            options.get("output_root") or "artifacts/support-escalation",
            {
                "execution": (
                    "actual_owned_Anthropic_Pipedream"
                    if mode == "pipedream"
                    else "actual_Anthropic_SDK_owned_HTTP_local_queue"
                ),
                "action_mode": mode if type(mode) is str else None,
                "caller_event": payload if type(payload) is dict else None,
                "allow_writes": writes if type(writes) is bool else None,
                "allocation": {"source": False, "scope": False, "provider": False},
                "observed_business": {
                    "status": "unopened",
                    "actual_external_delivery": None,
                },
            },
        )
        if type(timeout_ms) is not int or not 10 <= timeout_ms <= 300000:
            raise ValueError("bounded exact support timeout_ms10..300000 required")
        owner = Run(receipt, timeout_ms)
        if mode not in ("local", "pipedream") or type(pipe_allowed) is not bool:
            raise ValueError(
                "explicit local or Pipedream mode and action permission required"
            )
        owner.mode = mode
        if set(options) - allowed:
            raise ValueError(
                "unsupported support runtime options: "
                + ", ".join(sorted(set(options) - allowed))
            )
        event(payload)
        # Check the complete original payload before allocation, including JSON bounds.
        parse_json(canonical(payload).encode("utf-8", errors="strict"))
        if type(writes) is not bool:
            raise ValueError("exact separate support write permission required")
        if not isinstance(database, (str, Path)):
            raise ValueError("explicit managed support database required")
        for signal in signals:
            if signal is not None and not isinstance(signal, asyncio.Event):
                raise ValueError("actual asyncio cancellation Event required")
            if signal is not None and signal.is_set():
                raise RuntimeError("support caller already cancelled")
        owner.stop_events = [signal for signal in signals if signal is not None]
        pipe_config = (
            delivery_config(
                pipe_settings, allow_actions=pipe_allowed, allow_writes=writes
            )
            if mode == "pipedream"
            else None
        )
        if mode == "local" and (pipe_settings is not None or pipe_allowed):
            raise ValueError("local mode cannot accept remote action configuration")
        if pipe_config is not None:
            receipt.data["delivery_configuration"] = {
                "destination": pipe_config.destination(),
                "action_permission": True,
                "sdk_max_retries": 0,
                "oauth_request_limit": 1,
                "action_request_limit": 1,
                "actual_delivery": None,
            }
        configuration = (
            config(api_key, model, base_url)
            if payload["kind"] == "clock_tick"
            else None
        )
        if configuration is not None:
            receipt.data["provider_configuration"] = {
                "model": configuration.model,
                "base_url": configuration.base_url,
                "key": "provided",
                "max_tokens": 256,
                "prepared_request_limit": 1,
                "max_retries": 0,
            }

        async def watch(signal, label):
            await signal.wait()
            owner.cancel(RuntimeError(label + " cancelled support event"))

        for signal, label in zip(signals, ("caller", "parent")):
            if signal is not None:
                watchers.append(asyncio.create_task(watch(signal, label)))
        deadline_timer = asyncio.get_running_loop().call_later(
            timeout_ms / 1000,
            lambda: owner.cancel(TimeoutError("support shared deadline exceeded")),
        )
        owner.basic()
        owner.source_attempted = True
        try:
            owner.source = ManagedSource(database, commit_guard=owner.basic)
        except BaseException as error:
            owner.source = getattr(error, "managed_source", None)
            receipt.data["allocation"]["source"] = owner.source is not None and (
                owner.source.database is not None or owner.source.descriptor is not None
            )
            raise
        receipt.data["allocation"]["source"] = True
        initial = owner.source.snapshot()
        receipt.data["initial_source"] = receipt.source(initial)
        prior = next(
            (
                r
                for r in initial["business"]["events"]
                if r["payload"]["event_id"] == payload["event_id"]
            ),
            None,
        )
        if prior is not None:
            if prior["payload"] != payload:
                raise ValueError("event ID reused with different original payload")
            outcome = {
                "status": (
                    "review_required"
                    if prior["status"]
                    in ("eligible", "notification_started", "review_required")
                    else "already_recorded"
                ),
                "prior_status": prior["status"],
                "reason": "original event is preserved and no model or notification is replayed",
            }
        elif payload["kind"] != "clock_tick":
            current_intent = owner.source.intent(payload) if intent is None else intent
            if writes:
                owner.check()
                decision = owner.source.event(payload, intent=current_intent)
                receipt.data["acknowledged_decisions"].append(deepcopy(decision))
                if decision["decision"]["status"] == "applied":
                    receipt.data["acknowledged_effects"].append(deepcopy(decision))
                receipt.data["actions"].append(
                    {
                        "kind": "explicit_caller_update",
                        "decision": deepcopy(decision),
                        "source": receipt.source(owner.source.snapshot()),
                    }
                )
                owner.save()
                outcome = decision["result"]
            else:
                outcome = {"status": "write_permission_required"}
        else:
            current_intent = owner.source.intent(payload) if intent is None else intent
            owner.scope = owner.source.begin(
                payload,
                current_intent,
                TASK,
                allow_writes=writes,
                mode=mode,
                destination=(
                    pipe_config.destination() if pipe_config is not None else None
                ),
            )
            receipt.data["allocation"]["scope"] = True
            first = owner.observe()
            outcome = owner.apply({"kind": "event", "args": payload}, first)
            current = owner.observe()
            if payload["kind"] == "clock_tick" and current["value"]["eligible"]:
                owner.check()
                try:
                    owner.native = Native(owner, configuration, current["value"])
                except BaseException as error:
                    owner.native = getattr(error, "native_owner", None)
                    receipt.data["allocation"]["provider"] = (
                        owner.native is not None
                        and (
                            owner.native.transport is not None
                            or owner.native.http is not None
                            or owner.native.client is not None
                        )
                    )
                    raise
                receipt.data["allocation"]["provider"] = True
                owner.job = asyncio.create_task(owner.native.summarize())
                owner.flow_drained = False
                summary = await asyncio.shield(owner.job)
                owner.flow_drained = True
                owner.check()
                if pipe_config is not None:
                    try:
                        owner.delivery = Delivery(
                            owner,
                            pipe_config,
                            payload["event_id"],
                            payload["ticket_id"],
                            summary,
                        )
                    except BaseException as error:
                        owner.delivery = getattr(error, "delivery_owner", None)
                        receipt.data["delivery_allocated"] = (
                            owner.delivery is not None
                            and (
                                owner.delivery.transport is not None
                                or owner.delivery.http is not None
                            )
                        )
                        raise
                    receipt.data["delivery_allocated"] = True
                try:
                    outcome = owner.apply(
                        {
                            "kind": "reserve",
                            "args": {
                                "event_id": payload["event_id"],
                                "source": current["value"]["ticket"],
                                "summary": summary,
                                "mode": mode,
                                "destination": (
                                    pipe_config.destination()
                                    if pipe_config is not None
                                    else None
                                ),
                            },
                        },
                        current,
                    )
                except ValueError as error:
                    # A genuine concurrent human event invalidates the old draft read.
                    fresh = owner.source.snapshot()
                    if (
                        fresh["context_sha256"]
                        == current["observation"]["context_sha256"]
                    ):
                        raise
                    reinspection = owner.observe()
                    outcome = owner.apply(
                        {
                            "kind": "reserve",
                            "args": {
                                "event_id": payload["event_id"],
                                "source": reinspection["value"]["ticket"],
                                "summary": summary,
                                "mode": mode,
                                "destination": (
                                    pipe_config.destination()
                                    if pipe_config is not None
                                    else None
                                ),
                            },
                        },
                        reinspection,
                    )
                    receipt.data["changed_during_draft"] = {
                        "error": error_info(error),
                        "original_context_sha256": current["observation"][
                            "context_sha256"
                        ],
                    }
                owner.delivery_read = owner.observe()
                if mode == "pipedream" and outcome["status"] == "notification_started":
                    owner.job = asyncio.create_task(owner.delivery.notify())
                    owner.flow_drained = False
                    remote = await asyncio.shield(owner.job)
                    owner.flow_drained = True
                    owner.delivery_check(owner.delivery)
                    outcome = owner.apply(
                        {
                            "kind": "notification_observation",
                            "args": {
                                "event_id": payload["event_id"],
                                "status": "remote_acknowledged",
                                "response_sha256": remote["response_sha256"],
                                "observation_kind": OWNED_OBSERVATION,
                            },
                        },
                        owner.delivery_read,
                    )
                    owner.observe()
    except BaseException as error:
        if receipt is None:
            receipt = getattr(error, "receipt", None)
        if owner is None and receipt is not None:
            owner = Run(receipt, 120000)
        if owner is not None:
            owner.fail(error)
        else:
            raise
    finally:
        if owner is not None:
            owner.closing = True
            owner.close_scope()

            async def finish():
                owner.closing = True
                owner.close_scope()
                if owner.job is not None:
                    if not owner.job.done():
                        receipt.data["lifecycle"].append(
                            {
                                "kind": "flow_cancel_requested",
                                "scope_closed": owner.scope_closed,
                            }
                        )
                        owner.job.cancel()
                    await asyncio.gather(owner.job, return_exceptions=True)
                    owner.flow_drained = True
                provider_close_confirmed = owner.native is None
                delivery_close_confirmed = owner.delivery is None
                if owner.delivery is not None:
                    try:
                        await owner.delivery.close()
                        delivery_close_confirmed = owner.delivery.closed
                    except BaseException as error:
                        owner.fail(error)
                        receipt.data["delivery_close_error"] = error_info(error)
                    receipt.data["delivery_state"] = {
                        "oauth_prepared": owner.delivery.oauth_prepared,
                        "action_prepared": owner.delivery.action_prepared,
                        "action_attempted": owner.delivery.action_attempted,
                        "original_action_acknowledged": owner.delivery.observation
                        is not None,
                        "actual_delivery": None,
                    }
                if owner.native is not None:
                    try:
                        await owner.native.close()
                        provider_close_confirmed = owner.native.closed
                    except BaseException as error:
                        owner.fail(error)
                        receipt.data["provider_close_error"] = error_info(error)
                if owner.source is not None:
                    try:
                        final = owner.source.snapshot()
                        receipt.data["final_source"] = receipt.source(final)
                        receipt.data["observed_business"] = observed(final, payload)
                    except BaseException as error:
                        receipt.data["observed_business"] = {
                            "status": "unknown",
                            "actual_external_delivery": None,
                            "error": error_info(error),
                        }
                        owner.fail(error)
                    try:
                        cleanup = owner.source.close()
                        source_close_confirmed = True
                    except BaseException as error:
                        owner.fail(error)
                        source_close_confirmed = False
                        cleanup = {
                            "database_close_attempted": owner.source.database_close_attempted,
                            "database_closed": owner.source.database_closed,
                            "descriptor_close_attempted": owner.source.descriptor_close_attempted,
                            "descriptor_closed": owner.source.descriptor_closed,
                            "error": error_info(error),
                        }
                else:
                    cleanup = None
                    source_close_confirmed = True
                for watcher in watchers:
                    watcher.cancel()
                if watchers:
                    await asyncio.gather(*watchers, return_exceptions=True)
                if deadline_timer is not None:
                    deadline_timer.cancel()
                if time.monotonic() >= owner.deadline:
                    owner.fail(TimeoutError("support deadline exceeded during cleanup"))
                receipt.data["cleanup"] = {
                    "scope_close_attempted": owner.scope_close_attempted,
                    "scope_closed": owner.scope_closed,
                    "flow_drained": owner.flow_drained,
                    "provider_close_attempted": (
                        owner.native.close_attempted
                        if owner.native is not None
                        else False
                    ),
                    "provider_closed": (
                        owner.native.closed if owner.native is not None else True
                    ),
                    "provider_close_confirmed": provider_close_confirmed,
                    "source_close_confirmed": source_close_confirmed,
                    "delivery_close_attempted": (
                        owner.delivery.close_attempted
                        if owner.delivery is not None
                        else False
                    ),
                    "delivery_closed": (
                        owner.delivery.closed if owner.delivery is not None else True
                    ),
                    "delivery_close_confirmed": delivery_close_confirmed,
                    "source": cleanup,
                }
                receipt.data["cleanup_confirmed"] = (
                    owner.scope_closed
                    and owner.flow_drained
                    and provider_close_confirmed
                    and delivery_close_confirmed
                    and source_close_confirmed
                    and (
                        cleanup is None
                        or (
                            cleanup["database_closed"]
                            or not cleanup["database_close_attempted"]
                        )
                        and (
                            cleanup["descriptor_closed"]
                            or not cleanup["descriptor_close_attempted"]
                        )
                    )
                )
                if not receipt.data["cleanup_confirmed"]:
                    owner.fail(RuntimeError("support cleanup is unconfirmed"))
                receipt.data["source_attempted"] = owner.source_attempted
                receipt.data["finished_at"] = datetime.now(timezone.utc).isoformat()
                receipt.data["status"] = "failed" if owner.failure else "completed"
                receipt.data["error"] = (
                    error_info(owner.failure) if owner.failure else None
                )
                result = (
                    {
                        "status": "read_only" if not writes else outcome["status"],
                        "outcome": outcome,
                        "summary": summary,
                        "business": receipt.data["observed_business"],
                        "receipt_path": str(receipt.path),
                        "cleanup_confirmed": receipt.data["cleanup_confirmed"],
                        "usage": (
                            receipt.data["calls"][0].get("reported_usage")
                            if receipt.data["calls"]
                            else None
                        ),
                        "cost_usd": None,
                        "actual_external_delivery": None,
                    }
                    if owner.failure is None
                    else None
                )
                receipt.data["result"] = result
                try:
                    owner.save()
                    if time.monotonic() >= owner.deadline:
                        raise TimeoutError(
                            "support deadline exceeded during final recording"
                        )
                except BaseException as error:
                    owner.fail(error)
                    receipt.data["status"] = "failed"
                    receipt.data["result"] = None
                    receipt.data["error"] = error_info(owner.failure)
                    if receipt.error is None:
                        try:
                            receipt.save()
                        except BaseException:
                            pass

            cleanup_task = asyncio.create_task(finish())
            while not cleanup_task.done():
                try:
                    await asyncio.shield(cleanup_task)
                except asyncio.CancelledError as error:
                    owner.fail(error)
            cleanup_task.result()
    if owner.failure is not None:
        original = owner.failure
        error = (
            RuntimeError("support caller cancelled")
            if isinstance(original, asyncio.CancelledError)
            else original
        )
        error.evidence = receipt.data
        error.receipt_path = str(receipt.path) if receipt.path else None
        if error is not original:
            raise error from original
        raise error
    return receipt.data["result"]


async def run_local_event(**options):
    """The local entry point can never select an external action mode."""
    if options.get("action_mode", "local") != "local":
        raise ValueError("local event entry point cannot dispatch external actions")
    return await run_event(**{**options, "action_mode": "local"})
