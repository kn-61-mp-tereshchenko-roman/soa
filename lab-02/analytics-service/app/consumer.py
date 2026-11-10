import json
import logging
import os
from datetime import datetime, timezone
from uuid import UUID

import pika
import psycopg


logging.basicConfig(level=logging.INFO)

DATABASE_URL = os.environ["DATABASE_URL"]

RABBITMQ_HOST = os.environ["RABBITMQ_HOST"]
RABBITMQ_USER = os.environ["RABBITMQ_USER"]
RABBITMQ_PASSWORD = os.environ["RABBITMQ_PASSWORD"]

EXCHANGE_NAME = "nutrition.events"
QUEUE_NAME = "analytics.meals"
ROUTING_KEY = "meal.created"

DEAD_EXCHANGE = "nutrition.dead"
DEAD_QUEUE = "analytics.meals.dead"
DEAD_ROUTING_KEY = "meals.dead"


def get_connection():
    return psycopg.connect(DATABASE_URL)


def initialize_database():
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_nutrition_summary (
                user_id UUID NOT NULL,
                summary_date DATE NOT NULL,
                total_calories DOUBLE PRECISION NOT NULL DEFAULT 0,
                total_protein DOUBLE PRECISION NOT NULL DEFAULT 0,
                total_fat DOUBLE PRECISION NOT NULL DEFAULT 0,
                total_carbs DOUBLE PRECISION NOT NULL DEFAULT 0,
                PRIMARY KEY (user_id, summary_date)
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_events (
                event_id UUID PRIMARY KEY,
                processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )


def process_event(event):
    if event.get("event_type") != "MealEntryCreated":
        raise ValueError("Unsupported event type")

    event_id = UUID(event["event_id"])
    data = event["data"]

    user_id = UUID(data["user_id"])

    consumed_at = datetime.fromisoformat(
        data["consumed_at"].replace("Z", "+00:00")
    )

    if consumed_at.tzinfo is None:
        raise ValueError("consumed_at must include a timezone")

    summary_date = consumed_at.astimezone(
        timezone.utc
    ).date()

    calories = float(data["calories"])
    protein = float(data["protein"])
    fat = float(data["fat"])
    carbs = float(data["carbs"])

    if min(calories, protein, fat, carbs) < 0:
        raise ValueError("Nutrition values cannot be negative")

    with get_connection() as conn:
        inserted = conn.execute(
            """
            INSERT INTO processed_events (event_id)
            VALUES (%s)
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
            """,
            (event_id,),
        ).fetchone()

        if inserted is None:
            logging.info(
                "Duplicate event ignored: %s",
                event_id,
            )
            return

        conn.execute(
            """
            INSERT INTO daily_nutrition_summary (
                user_id,
                summary_date,
                total_calories,
                total_protein,
                total_fat,
                total_carbs
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id, summary_date)
            DO UPDATE SET
                total_calories =
                    daily_nutrition_summary.total_calories
                    + EXCLUDED.total_calories,
                total_protein =
                    daily_nutrition_summary.total_protein
                    + EXCLUDED.total_protein,
                total_fat =
                    daily_nutrition_summary.total_fat
                    + EXCLUDED.total_fat,
                total_carbs =
                    daily_nutrition_summary.total_carbs
                    + EXCLUDED.total_carbs
            """,
            (
                user_id,
                summary_date,
                calories,
                protein,
                fat,
                carbs,
            ),
        )

    logging.info(
        "Processed event=%s user=%s date=%s calories=%.2f",
        event_id,
        user_id,
        summary_date,
        calories,
    )


def main():
    initialize_database()

    credentials = pika.PlainCredentials(
        RABBITMQ_USER,
        RABBITMQ_PASSWORD,
    )

    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=5672,
        credentials=credentials,
        heartbeat=60,
        connection_attempts=10,
        retry_delay=3,
    )

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.exchange_declare(
        exchange=EXCHANGE_NAME,
        exchange_type="direct",
        durable=True,
    )

    channel.exchange_declare(
        exchange=DEAD_EXCHANGE,
        exchange_type="direct",
        durable=True,
    )

    channel.queue_declare(
        queue=DEAD_QUEUE,
        durable=True,
    )

    channel.queue_bind(
        exchange=DEAD_EXCHANGE,
        queue=DEAD_QUEUE,
        routing_key=DEAD_ROUTING_KEY,
    )

    channel.queue_declare(
        queue=QUEUE_NAME,
        durable=True,
        arguments={
            "x-dead-letter-exchange": DEAD_EXCHANGE,
            "x-dead-letter-routing-key": DEAD_ROUTING_KEY,
        },
    )

    channel.queue_bind(
        exchange=EXCHANGE_NAME,
        queue=QUEUE_NAME,
        routing_key=ROUTING_KEY,
    )

    channel.basic_qos(prefetch_count=10)

    def callback(ch, method, properties, body):
        try:
            event = json.loads(body)
            process_event(event)

        except Exception:
            logging.exception("Failed to process message")

            ch.basic_nack(
                delivery_tag=method.delivery_tag,
                requeue=False,
            )

        else:
            ch.basic_ack(
                delivery_tag=method.delivery_tag,
            )

    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=callback,
        auto_ack=False,
    )

    logging.info("Waiting for meal events")

    try:
        channel.start_consuming()

    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
