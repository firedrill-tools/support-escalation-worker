"""Webhook surface with a caller token and explicit clock ticks."""

from typing import Literal
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from .service import Worker


class TicketEvent(BaseModel):
    event_id: str = Field(min_length=1, max_length=100)
    ticket_id: str = Field(min_length=1, max_length=100)
    kind: Literal["clock_tick", "agent_reply", "customer_reply", "resolved"]
    occurred_at: str


def create_app(worker: Worker, token: str) -> FastAPI:
    if not token:
        raise ValueError("EVENT_TOKEN required")
    app = FastAPI(title="Synthetic Support Escalation")

    @app.post("/events")
    def events(payload: TicketEvent, x_event_token: str | None = Header(default=None)):
        if x_event_token != token:
            raise HTTPException(status_code=401, detail="invalid event token")
        try:
            result = worker.handle(payload.model_dump())
        except ValueError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        except RuntimeError as error:
            raise HTTPException(status_code=503, detail=str(error)) from error
        return result

    return app
