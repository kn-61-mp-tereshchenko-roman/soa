import json
import logging
import os
import time

import pika
import psycopg
from psycopg.rows import dict_row


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [OUTBOX] %(levelname)s: %(message)s",
)

DATABASE_URL = os.environ["DATABASE_URL"]

RABBITMQ_HOST = os.environ["RABBITMQ_HOST"]
RABBITMQ_USER = os.environ["RABBITMQ_USER"]
RABBITMQ_PASSWORD = os.environ["RABBITMQ_PASSWORD"]

EXCHANGE_NAME = "nutrition.events"
ROUTING_KEY = "meal.created"

BATCH_SIZE = 10
POLL_INTERVAL = 2
LOCK_SECONDS = 30
RECONNECT_DELAY = 5


def connect_rabbitmq():
    credentials = pika.PlainCredentials(
        RABBITMQ_USER,
        RABBITMQ_PASSWORD,
    )

    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=5672,
        credentials=credentials,
        heartbeat=60,
        blocked_connection_timeout=30,
        connection_attempts=5,
        retry_delay=2,
        socket_timeout=5,
    )

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.exchange_declare(
        exchange=EXCHANGE_NAME,
        exchange_type="direct",
        durable=True,
    )

    channel.confirm_delivery()

    return connection, channel


def initialize_database():
    with psycopg.connect(DATABASE_URL) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                ALTER TABLE outbox_events
                ADD COLUMN IF NOT EXISTS locked_until TIMESTAMPTZ
                """
            )

    logging.info("Outbox database structure is ready.")


def claim_events():
    with psycopg.connect(
        DATABASE_URL,
        row_factory=dict_row,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    event_id,
                    event_type,
                    payload,
                    attempts
                FROM outbox_events
                WHERE published_at IS NULL
                  AND (
                      locked_until IS NULL
                      OR locked_until < NOW()
                  )
                ORDER BY created_at
                LIMIT %s
                FOR UPDATE SKIP LOCKED
                """,
                (BATCH_SIZE,),
            )

            events = cursor.fetchall()

            if not events:
                return []

            event_ids = [event["event_id"] for event in events]

            cursor.execute(
                """
                UPDATE outbox_events
                SET
                    locked_until = NOW() + (%s * INTERVAL '1 second'),
                    attempts = attempts + 1
                WHERE event_id = ANY(%s)
                """,
                (LOCK_SECONDS, event_ids),
            )

            return events


def mark_published(event_id):
    with psycopg.connect(DATABASE_URL) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE outbox_events
                SET
                    published_at = NOW(),
                    locked_until = NULL,
                    last_error = NULL
                WHERE event_id = %s
                """,
                (event_id,),
            )


def mark_failed(event_id, error):
    with psycopg.connect(DATABASE_URL) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE outbox_events
                SET
                    locked_until = NOW() + (%s * INTERVAL '1 second'),
                    last_error = %s
                WHERE event_id = %s
                """,
                (LOCK_SECONDS, str(error)[:2000], event_id),
            )


def publish_event(channel, event):
    payload = event["payload"]

    if isinstance(payload, str):
        payload = json.loads(payload)

    message = json.dumps(payload, default=str)

    channel.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=ROUTING_KEY,
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
            message_id=str(event["event_id"]),
            type=event["event_type"],
        ),
        mandatory=True,
    )


def run():
    initialize_database()

    while True:
        connection = None

        try:
            connection, channel = connect_rabbitmq()

            logging.info("Connected to RabbitMQ.")

            while connection.is_open and channel.is_open:
                events = claim_events()

                if not events:
                    # Process heartbeats and other AMQP events while idle.
                    connection.process_data_events(
                        time_limit=POLL_INTERVAL
                    )
                    continue

                for event in events:
                    event_id = event["event_id"]

                    try:
                        publish_event(channel, event)
                        mark_published(event_id)

                        logging.info(
                            "Published event %s (%s).",
                            event_id,
                            event["event_type"],
                        )

                    except Exception as error:
                        mark_failed(event_id, error)

                        logging.exception(
                            "Failed to publish event %s. "
                            "Reconnecting to RabbitMQ.",
                            event_id,
                        )

                        # Do not reuse a potentially closed channel.
                        raise

        except KeyboardInterrupt:
            logging.info("Outbox Publisher stopped.")
            break

        except Exception:
            logging.exception(
                "Outbox Publisher connection error. "
                "Retrying in %s seconds.",
                RECONNECT_DELAY,
            )
            time.sleep(RECONNECT_DELAY)

        finally:
            if connection is not None and connection.is_open:
                try:
                    connection.close()
                except Exception:
                    logging.exception(
                        "Error while closing RabbitMQ connection."
                    )


if __name__ == "__main__":
    run()
