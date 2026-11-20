"""A2A consumer agent: coordinates with the Producer Agent and reads analytics."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import date, datetime, timezone
from uuid import UUID

import httpx
import uvicorn
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError
from starlette.applications import Starlette

from a2a.client import ClientConfig, create_client
from a2a.helpers import get_message_text, new_task_from_user_message, new_text_message, new_text_part
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.routes import create_agent_card_routes, create_jsonrpc_routes
from a2a.server.tasks import InMemoryTaskStore, TaskUpdater
from a2a.types import AgentCapabilities, AgentCard, AgentInterface, AgentSkill, Role, SendMessageRequest, TaskState

load_dotenv()
API_BASE_URL = os.getenv("CALORIE_API_BASE_URL", "http://127.0.0.1:8080").rstrip("/")
PRODUCER_AGENT_URL = os.getenv("PRODUCER_AGENT_URL", "http://127.0.0.1:9991")
HOST = os.getenv("CONSUMER_HOST", "127.0.0.1")
PORT = int(os.getenv("CONSUMER_PORT", "9992"))


class OrchestrateCommand(BaseModel):
    action: str = Field(pattern="^create_meal_and_get_summary$")
    user_id: UUID
    food_id: UUID
    meal_type: str = Field(pattern="^(breakfast|lunch|dinner|snack)$")
    quantity_grams: float = Field(gt=0, le=10000)
    summary_date: date
    approved: bool


class AnalyticsConsumer:
    async def invoke(self, raw_request: str) -> str:
        if len(raw_request) > 2000:
            return json.dumps({"ok": False, "error": "Request exceeds 2000 characters."})
        try:
            data = OrchestrateCommand.model_validate(json.loads(raw_request))
            if data.approved is not True:
                return json.dumps({"ok": False, "error": "Explicit human approval is required."})
            producer_payload = {
                "action": "create_meal",
                "user_id": str(data.user_id),
                "food_id": str(data.food_id),
                "meal_type": data.meal_type,
                "quantity_grams": data.quantity_grams,
                "approved": True,
            }
            # Read the current summary before creating the meal. This baseline
            # lets us wait for this particular event, not merely any old summary.
            async with httpx.AsyncClient(base_url=API_BASE_URL, timeout=5.0) as api:
                baseline_response = await api.get("/api/analytics/daily", params={
                    "user_id": str(data.user_id),
                    "summary_date": data.summary_date.isoformat(),
                })
                baseline_response.raise_for_status()
                baseline = baseline_response.json().get("total_calories", 0.0)

            # Discover the producer through its Agent Card, then send an A2A task.
            producer_client = await create_client(
                agent=PRODUCER_AGENT_URL,
                client_config=ClientConfig(streaming=False),
            )
            try:
                message = new_text_message(json.dumps(producer_payload), role=Role.ROLE_USER)
                request = SendMessageRequest(message=message)
                producer_text = ""
                async for event in producer_client.send_message(request):
                    if hasattr(event, "model_dump"):
                        obj = event.model_dump(mode="json")
                        # A2A result may be a Task with artifacts or a Message.
                        for artifact in obj.get("artifacts", []):
                            for part in artifact.get("parts", []):
                                if part.get("text"):
                                    producer_text = part["text"]
                        for part in obj.get("parts", []):
                            if part.get("text"):
                                producer_text = part["text"]
            finally:
                await producer_client.close()

            if not producer_text:
                return json.dumps({"ok": False, "error": "Producer Agent returned no result."})
            producer_result = json.loads(producer_text)
            if not producer_result.get("ok"):
                return json.dumps({"ok": False, "producer": producer_result})

            # Lab 3 analytics is asynchronous (Outbox -> RabbitMQ -> Consumer).
            # Wait until the summary includes the calories from this meal.
            created_meal = producer_result.get("result", {})
            expected_total = baseline + float(created_meal.get("calories", 0.0))
            summary = None
            async with httpx.AsyncClient(base_url=API_BASE_URL, timeout=5.0) as api:
                for _ in range(20):
                    response = await api.get("/api/analytics/daily", params={
                        "user_id": str(data.user_id),
                        "summary_date": data.summary_date.isoformat(),
                    })
                    response.raise_for_status()
                    summary = response.json()
                    if summary.get("total_calories", 0.0) >= expected_total - 0.02:
                        break
                    await asyncio.sleep(0.5)
            analytics_updated = summary is not None and summary.get("total_calories", 0.0) >= expected_total - 0.02
            return json.dumps({
                "ok": True,
                "analytics_updated_for_this_meal": analytics_updated,
                "producer_agent_result": producer_result,
                "daily_analytics": summary,
                "note": "Analytics is updated asynchronously by the Lab 3 RabbitMQ consumer. If analytics_updated_for_this_meal is false, inspect the Outbox Publisher, Analytics Consumer, DLQ, and SAGA Coordinator logs.",
            }, ensure_ascii=False)
        except (json.JSONDecodeError, ValidationError, ValueError) as exc:
            return json.dumps({"ok": False, "error": f"Invalid request: {str(exc)[:400]}"})
        except httpx.HTTPError as exc:
            return json.dumps({"ok": False, "error": f"Analytics API error: {type(exc).__name__}"})
        except Exception as exc:
            return json.dumps({"ok": False, "error": f"A2A orchestration failed: {type(exc).__name__}: {str(exc)[:300]}"})


class ConsumerExecutor(AgentExecutor):
    def __init__(self) -> None:
        self.agent = AnalyticsConsumer()

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        task = context.current_task or new_task_from_user_message(context.message)
        if context.current_task is None:
            await event_queue.enqueue_event(task)
        updater = TaskUpdater(event_queue=event_queue, task_id=task.id, context_id=task.context_id)
        await updater.update_status(state=TaskState.TASK_STATE_WORKING, message=new_text_message("Analytics Consumer is contacting the Producer Agent."))
        result = await self.agent.invoke(get_message_text(context.message) or "")
        await updater.add_artifact(parts=[new_text_part(text=result, media_type="application/json")])
        await updater.update_status(state=TaskState.TASK_STATE_COMPLETED, message=new_text_message("Consumer orchestration completed."))

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        raise NotImplementedError("Cancellation is not implemented for this short-running demo agent.")


skill = AgentSkill(
    id="orchestrate_meal_and_analytics",
    name="Meal and analytics orchestration",
    description="Calls the Meal Producer Agent over A2A, then reads the daily summary updated asynchronously by the Lab 3 RabbitMQ analytics consumer.",
    tags=["calorie-tracking", "analytics", "consumer", "a2a", "rabbitmq"],
    examples=['{"action":"create_meal_and_get_summary","user_id":"UUID","food_id":"UUID","meal_type":"breakfast","quantity_grams":80,"summary_date":"2026-11-12","approved":true}'],
    input_modes=["text/plain"],
    output_modes=["application/json"],
)
card = AgentCard(
    name="Calorie Analytics Consumer Agent",
    description="A2A consumer agent that delegates meal creation to the Producer Agent and retrieves the resulting nutrition summary.",
    version="1.0.0",
    default_input_modes=["text/plain"],
    default_output_modes=["application/json"],
    capabilities=AgentCapabilities(streaming=False),
    supported_interfaces=[AgentInterface(protocol_binding="JSONRPC", url=f"http://{HOST}:{PORT}", protocol_version="1.0")],
    skills=[skill],
)
handler = DefaultRequestHandler(agent_executor=ConsumerExecutor(), task_store=InMemoryTaskStore(), agent_card=card)
app = Starlette(routes=[*create_agent_card_routes(card), *create_jsonrpc_routes(handler, "/")])

if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT)
