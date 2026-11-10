import json
import logging
import os
import threading
import time
from uuid import UUID

import httpx
import pika
import psycopg


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SAGA] %(levelname)s: %(message)s",
)

DATABASE_URL = os.environ["DATABASE_URL"]

RABBITMQ_HOST = os.environ["RABBITMQ_HOST"]
RABBITMQ_USER = os.environ["RABBITMQ_USER"]
RABBITMQ_PASSWORD = os.environ["RABBITMQ_PASSWORD"]

MEAL_SERVICE_URL = os.getenv(
    "MEAL_SERVICE_URL",
    "http://meal-service:8000",
)

DEAD_EXCHANGE = "nutrition.dead"
DEAD_QUEUE = "analytics.meals.dead"
DEAD_ROUTING_KEY = "meals.dead"

POLL_INTERVAL_SECONDS = 5


def get_connection():
    return psycopg.connect(DATABASE_URL)


def compensate_meal(meal_id: UUID, reason: str):
    """Ask Meal Service to execute the compensating transaction."""

    response = httpx.post(
        f"{MEAL_SERVICE_URL}/internal/sagas/{meal_id}/compensate",
        params={"reason": reason},
        timeout=5.0,
    )

    response.raise_for_status()

    result = response.json()

    logging.info(
        "Compensation result: meal=%s status=%s reason=%s",
        meal_id,
        result.get("status"),
        reason,
    )


def process_dead_letter(channel, method, properties, body):
    """Compensate a meal whose event was rejected by the consumer."""

    try:
        event = json.loads(body)
        meal_id = UUID(event["data"]["meal_id"])

    except (ValueError, KeyError, TypeError, json.JSONDecodeError):
        logging.exception(
            "Invalid dead-letter message; discarding it"
        )

        # The message cannot be used to identify a SAGA.
        channel.basic_ack(
            delivery_tag=method.delivery_tag,
        )
        return

    try:
        compensate_meal(
            meal_id,
            "Consumer rejected event; message received from DLQ",
        )

    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            logging.error(
                "SAGA not found for dead-letter meal %s; discarding",
                meal_id,
            )
            channel.basic_ack(
                delivery_tag=method.delivery_tag,
            )
            return

        logging.exception(
            "Meal Service returned an error; requeueing DLQ message"
        )
        channel.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=True,
        )

    except httpx.HTTPError:
        logging.exception(
            "Meal Service is unavailable; requeueing DLQ message"
        )
        channel.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=True,
        )

    else:
        channel.basic_ack(
            delivery_tag=method.delivery_tag,
        )


def timeout_monitor():
    """Find and compensate SAGA instances whose deadlines expired."""

    while True:
        try:
            with get_connection() as conn:
                rows = conn.execute(
                    """
                    SELECT meal_id
                    FROM saga_instances
                    WHERE status = 'PENDING'
                      AND deadline_at <= NOW()
                    ORDER BY deadline_at
                    LIMIT 50
                    """
                ).fetchall()

            for (meal_id,) in rows:
                try:
                    compensate_meal(
                        meal_id,
                        "SAGA transaction deadline expired",
                    )

                except httpx.HTTPError:
                    logging.exception(
                        "Could not compensate expired meal %s",
                        meal_id,
                    )

        except Exception:
            logging.exception(
                "Timeout monitor encountered an error"
            )

        time.sleep(POLL_INTERVAL_SECONDS)


def main():
    # Run the timeout monitor independently of RabbitMQ consumption.
    monitor = threading.Thread(
        target=timeout_monitor,
        daemon=True,
    )
    monitor.start()

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

    channel.basic_qos(prefetch_count=10)

    channel.basic_consume(
        queue=DEAD_QUEUE,
        on_message_callback=process_dead_letter,
        auto_ack=False,
    )

    logging.info(
        "SAGA coordinator started. Monitoring DLQ and expired transactions."
    )

    try:
        channel.start_consuming()

    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
