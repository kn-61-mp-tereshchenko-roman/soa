import json
import logging
import os
from datetime import datetime, timezone
from uuid import UUID

import pika
import psycopg


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s: %(message)s",
)

DATABASE_URL = os.environ["DATABASE_URL"]

RABBITMQ_HOST = os.environ["RABBITMQ_HOST"]
RABBITMQ_USER = os.environ["RABBITMQ_USER"]
RABBITMQ_PASSWORD = os.environ["RABBITMQ_PASSWORD"]

FORCE_CONSUMER_FAILURE = (
    os.getenv("FORCE_CONSUMER_FAILURE", "false").lower()
    in {"true", "1", "yes"}
)

EXCHANGE_NAME = "nutrition.events"
QUEUE_NAME = "analytics.meals"
ROUTING_KEY = "meal.created"

DEAD_EXCHANGE = "nutrition.dead"
DEAD_QUEUE = "analytics.meals.dead"
DEAD_ROUTING_KEY = "meals.dead"


def get_connection():
    """Create a PostgreSQL database connection."""
    return psycopg.connect(DATABASE_URL)


def initialize_database():
    """Create the analytics summary and Inbox tables."""
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

    logging.info("Analytics database structure is ready.")


def validate_event(event):
    """Validate and normalize a MealEntryCreated event."""

    if not isinstance(event, dict):
        raise ValueError("Event must be a JSON object")

    if event.get("event_type") != "MealEntryCreated":
        raise ValueError("Unsupported event type")

    event_id = UUID(event["event_id"])
    data = event["data"]

    if not isinstance(data, dict):
        raise ValueError("Event data must be a JSON object")

    meal_id = UUID(data["meal_id"])
    user_id = UUID(data["user_id"])

    consumed_at = datetime.fromisoformat(
        data["consumed_at"].replace("Z", "+00:00")
    )

    if consumed_at.tzinfo is None:
        raise ValueError("consumed_at must include a timezone")

    summary_date = consumed_at.astimezone(timezone.utc).date()

    calories = float(data["calories"])
    protein = float(data["protein"])
    fat = float(data["fat"])
    carbs = float(data["carbs"])

    if min(calories, protein, fat, carbs) < 0:
        raise ValueError("Nutrition values cannot be negative")

    return {
        "event_id": event_id,
        "meal_id": meal_id,
        "user_id": user_id,
        "summary_date": summary_date,
        "calories": calories,
        "protein": protein,
        "fat": fat,
        "carbs": carbs,
    }


def process_event(event):
    """Process an event using Inbox idempotency and one DB transaction."""

    normalized = validate_event(event)

    event_id = normalized["event_id"]
    meal_id = normalized["meal_id"]
    user_id = normalized["user_id"]

    calories = normalized["calories"]
    protein = normalized["protein"]
    fat = normalized["fat"]
    carbs = normalized["carbs"]

    summary_date = normalized["summary_date"]

    # Required laboratory failure scenario.
    # The exception occurs before any database changes.
    if FORCE_CONSUMER_FAILURE:
        raise RuntimeError(
            "Forced consumer failure for SAGA compensation test"
        )

    with get_connection() as conn:

        # First, register the event in the Inbox.
        # A duplicate event is ignored, even if its SAGA
        # has already reached COMPLETED status.
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

        # Lock the SAGA record to prevent concurrent
        # completion and compensation.
        saga = conn.execute(
            """
            SELECT saga_id, status
            FROM saga_instances
            WHERE meal_id = %s
            FOR UPDATE
            """,
            (meal_id,),
        ).fetchone()

        if saga is None:
            raise ValueError(
                f"SAGA instance not found for meal {meal_id}"
            )

        saga_id, saga_status = saga

        # A new event can only be processed by a pending SAGA.
        if saga_status != "PENDING":
            raise RuntimeError(
                f"Cannot process meal {meal_id}: "
                f"SAGA status is {saga_status}"
            )

        # Update the daily nutrition summary.
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

        # Activate the meal.
        meal_updated = conn.execute(
            """
            UPDATE meal_entries
            SET status = 'ACTIVE'
            WHERE id = %s
              AND status = 'PENDING'
            RETURNING id
            """,
            (meal_id,),
        ).fetchone()

        if meal_updated is None:
            raise RuntimeError(
                f"Could not activate pending meal {meal_id}"
            )

        # Complete the distributed SAGA transaction.
        saga_updated = conn.execute(
            """
            UPDATE saga_instances
            SET status = 'COMPLETED',
                updated_at = NOW(),
                failure_reason = NULL
            WHERE meal_id = %s
              AND status = 'PENDING'
            RETURNING saga_id
            """,
            (meal_id,),
        ).fetchone()

        if saga_updated is None:
            raise RuntimeError(
                f"Could not complete SAGA for meal {meal_id}"
            )

    # The database transaction has committed successfully.
    logging.info(
        "SAGA completed: saga=%s meal=%s event=%s "
        "user=%s calories=%.2f",
        saga_id,
        meal_id,
        event_id,
        user_id,
        calories,
    )


def create_rabbitmq_connection():
    """Connect to RabbitMQ and configure the consumer channel."""

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

    # Main event exchange.
    channel.exchange_declare(
        exchange=EXCHANGE_NAME,
        exchange_type="direct",
        durable=True,
    )

    # Dead Letter Exchange.
    channel.exchange_declare(
        exchange=DEAD_EXCHANGE,
        exchange_type="direct",
        durable=True,
    )

    # Dead Letter Queue.
    channel.queue_declare(
        queue=DEAD_QUEUE,
        durable=True,
    )

    channel.queue_bind(
        exchange=DEAD_EXCHANGE,
        queue=DEAD_QUEUE,
        routing_key=DEAD_ROUTING_KEY,
    )

    # Main event queue.
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

    return connection, channel


def main():
    initialize_database()

    if FORCE_CONSUMER_FAILURE:
        logging.warning(
            "FORCE_CONSUMER_FAILURE is enabled. "
            "Incoming events will deliberately fail."
        )

    connection, channel = create_rabbitmq_connection()

    def callback(ch, method, properties, body):
        try:
            event = json.loads(body)
            process_event(event)

        except Exception:
            logging.exception("Failed to process message")

            # Reject the message without requeueing.
            # RabbitMQ routes it to the configured DLQ.
            ch.basic_nack(
                delivery_tag=method.delivery_tag,
                requeue=False,
            )

        else:
            # Acknowledge only after successful processing
            # or successful duplicate detection.
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
