import json
import os
from datetime import datetime, timezone
from uuid import uuid4

import pika


RABBITMQ_HOST = os.environ["RABBITMQ_HOST"]
RABBITMQ_USER = os.environ["RABBITMQ_USER"]
RABBITMQ_PASSWORD = os.environ["RABBITMQ_PASSWORD"]

EXCHANGE_NAME = "nutrition.events"
ROUTING_KEY = "meal.created"


def publish_meal_created(data: dict):
    event = {
        "event_id": str(uuid4()),
        "event_type": "MealEntryCreated",
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "data": data,
    }

    credentials = pika.PlainCredentials(
        RABBITMQ_USER,
        RABBITMQ_PASSWORD,
    )

    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=5672,
        credentials=credentials,
        heartbeat=60,
        connection_attempts=3,
        retry_delay=2,
        socket_timeout=5,
    )

    connection = pika.BlockingConnection(parameters)

    try:
        channel = connection.channel()

        channel.exchange_declare(
            exchange=EXCHANGE_NAME,
            exchange_type="direct",
            durable=True,
        )

        channel.confirm_delivery()

        channel.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=ROUTING_KEY,
            body=json.dumps(event),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
                message_id=event["event_id"],
            ),
            mandatory=True,
        )

        return event

    finally:
        connection.close()
