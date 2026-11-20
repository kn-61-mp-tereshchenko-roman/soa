"""A2A producer agent: exposes Lab 3 Meal Service capabilities."""

from __future__ import annotations

import json
import os
from uuid import UUID

import httpx
import uvicorn
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError
from starlette.applications import Starlette

from a2a.helpers import get_message_text, new_task_from_user_message, new_text_message, new_text_part
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.routes import create_agent_card_routes, create_jsonrpc_routes
from a2a.server.tasks import InMemoryTaskStore, TaskUpdater
from a2a.types import AgentCapabilities, AgentCard, AgentInterface, AgentSkill, TaskState

load_dotenv()
API_BASE_URL = os.getenv("CALORIE_API_BASE_URL", "http://127.0.0.1:8080").rstrip("/")
HOST = os.getenv("PRODUCER_HOST", "127.0.0.1")
PORT = int(os.getenv("PRODUCER_PORT", "9991"))


class CreateMealCommand(BaseModel):
    action: str = Field(pattern="^create_meal$")
    user_id: UUID
    food_id: UUID
    meal_type: str = Field(pattern="^(breakfast|lunch|dinner|snack)$")
    quantity_grams: float = Field(gt=0, le=10000)
    approved: bool


class ListMealsCommand(BaseModel):
    action: str = Field(pattern="^list_meals$")
    user_id: UUID


class MealProducer:
    async def invoke(self, raw_request: str) -> str:
        if len(raw_request) > 2000:
            return json.dumps({"ok": False, "error": "Request exceeds 2000 characters."})
        try:
            payload = json.loads(raw_request)
            action = payload.get("action")
            async with httpx.AsyncClient(base_url=API_BASE_URL, timeout=6.0) as client:
                if action == "create_meal":
                    data = CreateMealCommand.model_validate(payload)
                    if data.approved is not True:
                        return json.dumps({"ok": False, "error": "Explicit approval is required."})
                    body = data.model_dump(mode="json", exclude={"action", "approved"})
                    response = await client.post("/api/meals", json=body)
                elif action == "list_meals":
                    data = ListMealsCommand.model_validate(payload)
                    response = await client.get("/api/meals", params={"user_id": str(data.user_id)})
                else:
                    return json.dumps({"ok": False, "error": "Unsupported action."})
                response.raise_for_status()
                return json.dumps({"ok": True, "result": response.json()}, ensure_ascii=False)
        except (json.JSONDecodeError, ValidationError, ValueError) as exc:
            return json.dumps({"ok": False, "error": f"Invalid request: {str(exc)[:400]}"})
        except httpx.HTTPStatusError as exc:
            return json.dumps({"ok": False, "error": f"REST API HTTP {exc.response.status_code}: {exc.response.text[:300]}"})
        except httpx.RequestError as exc:
            return json.dumps({"ok": False, "error": f"REST API unavailable: {type(exc).__name__}"})


class ProducerExecutor(AgentExecutor):
    def __init__(self) -> None:
        self.agent = MealProducer()

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        task = context.current_task or new_task_from_user_message(context.message)
        if context.current_task is None:
            await event_queue.enqueue_event(task)
        updater = TaskUpdater(event_queue=event_queue, task_id=task.id, context_id=task.context_id)
        await updater.update_status(state=TaskState.TASK_STATE_WORKING, message=new_text_message("Meal Producer is processing the command."))
        result = await self.agent.invoke(get_message_text(context.message) or "")
        await updater.add_artifact(parts=[new_text_part(text=result, media_type="application/json")])
        await updater.update_status(state=TaskState.TASK_STATE_COMPLETED, message=new_text_message("Producer task completed."))

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        raise NotImplementedError("Cancellation is not implemented for this short-running demo agent.")


skill = AgentSkill(
    id="meal_write_and_read",
    name="Meal entry operations",
    description="Create a meal entry through the Lab 3 Meal Service or list a user's meal entries. Creation starts the existing Transactional Outbox and SAGA workflow.",
    tags=["calorie-tracking", "meal", "producer", "outbox", "saga"],
    examples=['{"action":"create_meal","user_id":"UUID","food_id":"UUID","meal_type":"breakfast","quantity_grams":80,"approved":true}', '{"action":"list_meals","user_id":"UUID"}'],
    input_modes=["text/plain"],
    output_modes=["application/json"],
)
card = AgentCard(
    name="Calorie Meal Producer Agent",
    description="A2A agent exposing Lab 3 Meal Service producer operations.",
    version="1.0.0",
    default_input_modes=["text/plain"],
    default_output_modes=["application/json"],
    capabilities=AgentCapabilities(streaming=False),
    supported_interfaces=[AgentInterface(protocol_binding="JSONRPC", url=f"http://{HOST}:{PORT}", protocol_version="1.0")],
    skills=[skill],
)
handler = DefaultRequestHandler(agent_executor=ProducerExecutor(), task_store=InMemoryTaskStore(), agent_card=card)
app = Starlette(routes=[*create_agent_card_routes(card), *create_jsonrpc_routes(handler, "/")])

if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT)
