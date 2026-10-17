"""Actual AsyncPipedream2.1.20 and owned old-httpx; no automatic action retries."""

from __future__ import annotations

import asyncio
import base64
import os
from copy import deepcopy
from dataclasses import dataclass

import httpx

from .delivery_contract import acknowledgement, atom, destination, origin
from .evidence import error_info
from .strict import canonical, digest, integer, keys, parse_json, sha256

MAX_RESPONSE = 1024 * 1024
ENV = {
    "access_token": "PIPEDREAM_ACCESS_TOKEN",
    "client_id": "PIPEDREAM_CLIENT_ID",
    "client_secret": "PIPEDREAM_CLIENT_SECRET",
    "project_id": "PIPEDREAM_PROJECT_ID",
    "external_user_id": "PIPEDREAM_EXTERNAL_USER_ID",
    "auth_provision_id": "PIPEDREAM_SLACK_AUTH_PROVISION_ID",
    "channel": "PIPEDREAM_SLACK_CHANNEL",
    "base_url": "PIPEDREAM_BASE_URL",
    "project_environment": "PIPEDREAM_PROJECT_ENVIRONMENT",
}


@dataclass(frozen=True)
class Config:
    access_token: str | None
    client_id: str | None
    client_secret: str | None
    project_id: str
    external_user_id: str
    auth_provision_id: str
    channel: str
    base_url: str

    def destination(self):
        return destination(
            {
                "provider": "pipedream",
                "base_url": self.base_url,
                "project_environment": "development",
                "project_id": self.project_id,
                "external_user_id": self.external_user_id,
                "auth_provision_id": self.auth_provision_id,
                "channel": self.channel,
                "credential_sha256": digest(
                    {
                        "client_id": self.client_id,
                        "client_secret_sha256": (
                            sha256(self.client_secret.encode())
                            if self.client_secret is not None
                            else None
                        ),
                        "access_token_sha256": (
                            sha256(self.access_token.encode())
                            if self.access_token is not None
                            else None
                        ),
                    }
                ),
            }
        )


def config(settings=None, *, allow_actions=False, allow_writes=False):
    if allow_actions is not True or allow_writes is not True:
        raise ValueError(
            "separate explicit Pipedream development action and write permissions required"
        )
    values = (
        {k: os.environ.get(v) for k, v in ENV.items()}
        if settings is None
        else deepcopy(settings)
    )
    if type(values) is not dict or set(values) - set(ENV):
        raise ValueError("exact explicit Pipedream settings required")
    if values.get("project_environment") not in (None, "development"):
        raise ValueError("only the Pipedream development environment is supported")
    token = values.get("access_token")
    client = values.get("client_id")
    secret = values.get("client_secret")
    if token is not None:
        atom(token, 4096)
        if client is not None or secret is not None:
            raise ValueError(
                "choose explicit Pipedream access token or client credential pair"
            )
    else:
        atom(client, 1024)
        atom(secret, 4096)
    cfg = Config(
        token,
        client,
        secret,
        values.get("project_id"),
        values.get("external_user_id"),
        values.get("auth_provision_id"),
        values.get("channel"),
        origin(
            "https://api.pipedream.com"
            if values.get("base_url") is None
            else values["base_url"]
        ),
    )
    cfg.destination()
    return cfg


def masked(value, secrets):
    if type(value) is dict:
        return {masked(k, secrets): masked(v, secrets) for k, v in value.items()}
    if type(value) is list:
        return [masked(v, secrets) for v in value]
    if type(value) is str:
        for secret in secrets:
            if secret:
                value = value.replace(secret, "[credential redacted]")
    return value


class Transport(httpx.AsyncBaseTransport):
    def __init__(self, delivery):
        self.delivery = delivery
        self.close_attempted = False
        self.closed = False
        self.inner = httpx.AsyncHTTPTransport(
            trust_env=False,
            retries=0,
            limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
        )

    async def handle_async_request(self, request):
        d = self.delivery
        owner = d.owner
        cfg = d.config
        owner.delivery_check(d)
        raw = await request.aread()
        if len(raw) > 65536:
            raise ValueError("Pipedream request exceeds64KiB")
        body = parse_json(raw)
        if (
            request.method != "POST"
            or request.headers.get("x-pd-environment") != "development"
            or request.headers.get("x-fern-sdk-version") != "2.1.20"
        ):
            raise ValueError("exact owned Pipedream development request required")
        url = str(request.url)
        oauth = url == cfg.base_url + "/v1/oauth/token"
        action = url == cfg.base_url + "/v1/connect/" + cfg.project_id + "/actions/run"
        if oauth:
            if cfg.access_token is not None or d.oauth_prepared:
                raise ValueError("one explicit OAuth request allowed")
            if (
                body
                != {
                    "client_id": cfg.client_id,
                    "client_secret": cfg.client_secret,
                    "grant_type": "client_credentials",
                }
                or request.headers.get("authorization") is not None
            ):
                raise ValueError("exact frozen OAuth credential request required")
            d.oauth_prepared = True
        elif action:
            if (
                d.action_prepared
                or d.token is None
                or request.headers.get("authorization") != "Bearer " + d.token
                or body != d.body
            ):
                raise ValueError("one exact frozen authorized action request required")
            d.action_prepared = True
        else:
            raise ValueError(
                "unsupported Pipedream route; no fallback or extra request"
            )
        call = {
            "kind": "oauth" if oauth else "action",
            "url": url,
            "method": "POST",
            "request_sha256": sha256(raw),
            "request": masked(body, d.secrets()),
            "request_credentials_redacted": oauth,
            "http_body_complete": False,
            "response": None,
            "error": None,
        }
        if action and not any(s.encode() in raw for s in d.secrets() if s):
            call["request_base64"] = base64.b64encode(raw).decode()
        elif action:
            call["request_credentials_redacted"] = True
        owner.receipt.data.setdefault("delivery_calls", []).append(call)
        owner.save()
        response = None
        parts = []
        size = 0
        phase = None
        try:
            phase = asyncio.get_running_loop().call_later(
                30,
                lambda: owner.cancel(TimeoutError("Pipedream phase exceeded30seconds")),
            )
            owner.delivery_check(d)
            if action:
                d.action_attempted = True
            response = await self.inner.handle_async_request(request)
            call["status_code"] = response.status_code
            call["response_headers"] = {
                k: v
                for k, v in response.headers.items()
                if k in ("content-type", "content-length", "request-id", "x-request-id")
            }
            async for part in response.aiter_bytes():
                take = min(len(part), MAX_RESPONSE - size)
                parts.append(part[:take])
                size += take
                if take != len(part):
                    raise ValueError("original Pipedream response exceeds1MiB")
            call["http_body_complete"] = True
            raw_response = b"".join(parts)
            original = parse_json(raw_response)
            if (
                oauth
                and type(original) is dict
                and type(original.get("access_token")) is str
            ):
                d.observed_secret = original["access_token"]
            call["response"] = masked(original, d.secrets())
            if response.status_code != 200:
                raise ValueError("original Pipedream HTTP status is not200")
            if oauth:
                if type(original) is not dict or "error" in original:
                    raise ValueError("original complete OAuth response required")
                atom(original.get("access_token"), 4096)
                integer(original.get("expires_in"), 1)
                if original["expires_in"] > 31536000:
                    raise ValueError("bounded original OAuth expiry required")
                if original.get("token_type", "Bearer") not in ("Bearer", "bearer"):
                    raise ValueError("original OAuth bearer token type required")
                d.token = original["access_token"]
            else:
                observation = acknowledgement(original, cfg.destination())
                observation["response_sha256"] = sha256(raw_response)
                d.original_ret = deepcopy(original["ret"])
                d.observation = deepcopy(observation)
                owner.receipt.data.setdefault("remote_observations", []).append(
                    deepcopy(observation)
                )
            owner.save()
            return httpx.Response(
                200,
                headers={
                    "content-type": "application/json",
                    "content-length": str(len(raw_response)),
                },
                content=raw_response,
                request=request,
            )
        except BaseException as error:
            call["error"] = error_info(error)
            owner.fail(error)
            raise
        finally:
            if phase is not None:
                phase.cancel()
            captured = b"".join(parts)
            call["response_sha256"] = sha256(captured)
            call["captured_bytes"] = len(captured)
            sensitive = oauth or any(s.encode() in captured for s in d.secrets() if s)
            call["response_credentials_redacted"] = sensitive
            if "response_headers" in call:
                call["response_headers"] = masked(call["response_headers"], d.secrets())
            if not sensitive:
                call["response_base64"] = base64.b64encode(captured).decode()
            if response is not None:
                try:
                    await response.aclose()
                    call["response_closed"] = True
                except BaseException as error:
                    call["response_closed"] = False
                    owner.fail(error)
                    raise
            owner.save()

    async def aclose(self):
        if self.close_attempted:
            return
        self.close_attempted = True
        await self.inner.aclose()
        self.closed = True


class Delivery:
    def __init__(self, owner, configuration, event_id, ticket_id, summary):
        self.owner = owner
        self.config = configuration
        self.oauth_prepared = False
        self.action_prepared = False
        self.action_attempted = False
        self.token = configuration.access_token
        self.observed_secret = None
        self.original_ret = None
        self.observation = None
        self.transport = None
        self.http = None
        self.client = None
        self.close_attempted = False
        self.closed = False
        self.body = {
            "id": "slack_v2-post-message",
            "external_user_id": configuration.external_user_id,
            "configured_props": {
                "slack_v2": {"authProvisionId": configuration.auth_provision_id},
                "channel": configuration.channel,
                "text": f"Support escalation {ticket_id} ({event_id}): {summary}",
            },
        }
        try:
            from pipedream import AsyncPipedream

            self.transport = Transport(self)
            self.http = httpx.AsyncClient(
                transport=self.transport,
                trust_env=False,
                follow_redirects=False,
                timeout=httpx.Timeout(300),
            )
            self.client = AsyncPipedream(
                access_token=configuration.access_token,
                client_id=configuration.client_id,
                client_secret=configuration.client_secret,
                project_id=configuration.project_id,
                project_environment="development",
                base_url=configuration.base_url,
                workflow_domain="m.pipedream.net",
                headers={},
                timeout=300,
                max_retries=0,
                follow_redirects=False,
                httpx_client=self.http,
            )
        except BaseException as error:
            error.delivery_owner = self
            raise

    def secrets(self):
        return [
            v
            for v in (
                self.config.access_token,
                self.config.client_secret,
                self.token,
                self.observed_secret,
            )
            if v
        ]

    async def notify(self):
        self.owner.delivery_check(self)
        result = await self.client.actions.run(**self.body)
        normalized = result.model_dump(mode="json")
        self.owner.receipt.data["delivery_sdk_response"] = masked(
            normalized, self.secrets()
        )
        if self.observation is None or result.ret != self.original_ret:
            raise ValueError(
                "actual Pipedream SDK return differs from original acknowledged action"
            )
        self.owner.save()
        return deepcopy(self.observation)

    async def close(self):
        if self.close_attempted:
            return
        self.close_attempted = True
        if self.http is not None:
            await self.http.aclose()
        elif self.transport is not None:
            await self.transport.aclose()
        self.closed = self.transport is None or self.transport.closed
