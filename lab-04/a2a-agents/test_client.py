"""Send one orchestration request to the A2A Consumer Agent."""

import asyncio
import json
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from a2a.client import ClientConfig, create_client
from a2a.helpers import new_text_message
from a2a.types import Role, SendMessageRequest

load_dotenv()
CONSUMER_AGENT_URL = os.getenv("CONSUMER_AGENT_URL", "http://127.0.0.1:9992")


async def main() -> None:
    user_id = input("Existing profile UUID: ").strip()
    food_id = input("Existing food UUID: ").strip()
    quantity = float(input("Quantity in grams (e.g. 80): ").strip())
    meal_type = input("Meal type [breakfast/lunch/dinner/snack]: ").strip().lower()
    confirmation = input("Type YES to authorize creation of this meal: ").strip()
    if confirmation != "YES":
        print("Cancelled; no A2A request was sent.")
        return
    payload = {
        "action": "create_meal_and_get_summary",
        "user_id": user_id,
        "food_id": food_id,
        "meal_type": meal_type,
        "quantity_grams": quantity,
        "summary_date": datetime.now(timezone.utc).date().isoformat(),
        "approved": True,
    }
    client = await create_client(
        agent=CONSUMER_AGENT_URL,
        client_config=ClientConfig(streaming=False),
    )
    try:
        message = new_text_message(json.dumps(payload), role=Role.ROLE_USER)
        request = SendMessageRequest(message=message)
        async for event in client.send_message(request):
            print(event.model_dump_json(indent=2) if hasattr(event, "model_dump_json") else event)
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
