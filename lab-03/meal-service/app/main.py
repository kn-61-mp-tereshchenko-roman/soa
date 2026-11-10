import os
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import httpx
import psycopg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from psycopg.types.json import Jsonb


app = FastAPI(title="Meal Service")

DATABASE_URL = os.environ["DATABASE_URL"]
PROFILE_SERVICE_URL = os.environ["PROFILE_SERVICE_URL"]
FOOD_SERVICE_URL = os.environ["FOOD_SERVICE_URL"]

INSTANCE_ID = os.getenv("HOSTNAME", str(uuid4()))

SAGA_TIMEOUT_SECONDS = int(os.getenv("SAGA_TIMEOUT_SECONDS", "60"))


def get_connection():
    return psycopg.connect(DATABASE_URL)


@app.on_event("startup")
def initialize_database():
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS meal_entries (
                id UUID PRIMARY KEY,
                user_id UUID NOT NULL,
                food_id UUID NOT NULL,
                food_name TEXT NOT NULL,
                meal_type TEXT NOT NULL,
                quantity_grams DOUBLE PRECISION NOT NULL
                    CHECK (quantity_grams > 0),
                calories DOUBLE PRECISION NOT NULL,
                protein DOUBLE PRECISION NOT NULL,
                fat DOUBLE PRECISION NOT NULL,
                carbs DOUBLE PRECISION NOT NULL,
                consumed_at TIMESTAMPTZ NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING'
            )
            """
        )

        # Migration for databases created during Lab 2.
        conn.execute(
            """
            ALTER TABLE meal_entries
            ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'ACTIVE'
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS outbox_events (
                event_id UUID PRIMARY KEY,
                aggregate_id UUID NOT NULL,
                event_type TEXT NOT NULL,
                payload JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                published_at TIMESTAMPTZ,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT
            )
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
            ON outbox_events (created_at)
            WHERE published_at IS NULL
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS saga_instances (
                saga_id UUID PRIMARY KEY,
                meal_id UUID NOT NULL UNIQUE,
                status TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                deadline_at TIMESTAMPTZ NOT NULL,
                failure_reason TEXT
            )
            """
        )


class MealCreate(BaseModel):
    user_id: UUID
    food_id: UUID
    meal_type: str
    quantity_grams: float = Field(gt=0, le=10000)


def request_remote_json(url: str, unavailable_message: str):
    try:
        response = httpx.get(url, timeout=3.0)

    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=503,
            detail=unavailable_message,
        ) from exc

    if response.status_code == 404:
        raise HTTPException(
            status_code=404,
            detail="Referenced resource not found",
        )

    if response.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail="Remote service returned an unexpected response",
        )

    return response.json()


@app.get("/api/meals/health")
def health():
    return {
        "status": "ok",
        "service": "meal-service",
        "instance_id": INSTANCE_ID,
    }


@app.get("/api/meals/debug/instance")
def get_instance():
    return {
        "service": "meal-service",
        "instance_id": INSTANCE_ID,
    }


@app.post("/api/meals", status_code=201)
def create_meal(data: MealCreate):
    if data.meal_type not in {
        "breakfast",
        "lunch",
        "dinner",
        "snack",
    }:
        raise HTTPException(
            status_code=422,
            detail="Unsupported meal type",
        )

    request_remote_json(
        f"{PROFILE_SERVICE_URL}/api/profiles/{data.user_id}",
        "Profile Service is unavailable",
    )

    food = request_remote_json(
        f"{FOOD_SERVICE_URL}/api/foods/{data.food_id}",
        "Food Catalog Service is unavailable",
    )

    factor = data.quantity_grams / 100.0

    calories = round(food["calories_per_100g"] * factor, 2)
    protein = round(food["protein_per_100g"] * factor, 2)
    fat = round(food["fat_per_100g"] * factor, 2)
    carbs = round(food["carbs_per_100g"] * factor, 2)

    meal_id = uuid4()
    event_id = uuid4()
    saga_id = uuid4()

    consumed_at = datetime.now(timezone.utc)
    deadline_at = consumed_at + timedelta(
        seconds=SAGA_TIMEOUT_SECONDS
    )

    record = {
        "id": str(meal_id),
        "user_id": str(data.user_id),
        "food_id": str(data.food_id),
        "food_name": food["name"],
        "meal_type": data.meal_type,
        "quantity_grams": data.quantity_grams,
        "calories": calories,
        "protein": protein,
        "fat": fat,
        "carbs": carbs,
        "consumed_at": consumed_at.isoformat(),
        "status": "PENDING",
    }

    event_data = {
        "meal_id": str(meal_id),
        "user_id": str(data.user_id),
        "food_id": str(data.food_id),
        "food_name": food["name"],
        "meal_type": data.meal_type,
        "quantity_grams": data.quantity_grams,
        "calories": calories,
        "protein": protein,
        "fat": fat,
        "carbs": carbs,
        "consumed_at": consumed_at.isoformat(),
    }

    event = {
        "event_id": str(event_id),
        "event_type": "MealEntryCreated",
        "occurred_at": consumed_at.isoformat(),
        "data": event_data,
    }

    try:
        # Business record, Outbox event and SAGA state are committed
        # together in one PostgreSQL transaction.
        with get_connection() as conn:
            conn.execute(
                """
                INSERT INTO meal_entries (
                    id, user_id, food_id, food_name, meal_type,
                    quantity_grams, calories, protein, fat, carbs,
                    consumed_at, status
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, 'PENDING'
                )
                """,
                (
                    meal_id,
                    data.user_id,
                    data.food_id,
                    food["name"],
                    data.meal_type,
                    data.quantity_grams,
                    calories,
                    protein,
                    fat,
                    carbs,
                    consumed_at,
                ),
            )

            conn.execute(
                """
                INSERT INTO outbox_events (
                    event_id,
                    aggregate_id,
                    event_type,
                    payload
                )
                VALUES (%s, %s, %s, %s)
                """,
                (
                    event_id,
                    meal_id,
                    "MealEntryCreated",
                    Jsonb(event),
                ),
            )

            conn.execute(
                """
                INSERT INTO saga_instances (
                    saga_id,
                    meal_id,
                    status,
                    deadline_at
                )
                VALUES (%s, %s, 'PENDING', %s)
                """,
                (
                    saga_id,
                    meal_id,
                    deadline_at,
                ),
            )

    except psycopg.Error as exc:
        raise HTTPException(
            status_code=500,
            detail="Could not create meal and its transaction records",
        ) from exc

    return {
        **record,
        "event_id": str(event_id),
        "saga_id": str(saga_id),
        "message": (
            "Meal and event saved transactionally. "
            "Event is waiting in the outbox."
        ),
    }


@app.get("/api/meals")
def list_meals(user_id: UUID):
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                id, user_id, food_id, food_name, meal_type,
                quantity_grams, calories, protein, fat, carbs,
                consumed_at, status
            FROM meal_entries
            WHERE user_id = %s
            ORDER BY consumed_at DESC
            """,
            (user_id,),
        ).fetchall()

    return [
        {
            "id": str(row[0]),
            "user_id": str(row[1]),
            "food_id": str(row[2]),
            "food_name": row[3],
            "meal_type": row[4],
            "quantity_grams": row[5],
            "calories": row[6],
            "protein": row[7],
            "fat": row[8],
            "carbs": row[9],
            "consumed_at": row[10].isoformat(),
            "status": row[11],
        }
        for row in rows
    ]


@app.get("/api/meals/{meal_id}")
def get_meal(meal_id: UUID):
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT
                id, user_id, food_id, food_name, meal_type,
                quantity_grams, calories, protein, fat, carbs,
                consumed_at, status
            FROM meal_entries
            WHERE id = %s
            """,
            (meal_id,),
        ).fetchone()

    if row is None:
        raise HTTPException(
            status_code=404,
            detail="Meal entry not found",
        )

    return {
        "id": str(row[0]),
        "user_id": str(row[1]),
        "food_id": str(row[2]),
        "food_name": row[3],
        "meal_type": row[4],
        "quantity_grams": row[5],
        "calories": row[6],
        "protein": row[7],
        "fat": row[8],
        "carbs": row[9],
        "consumed_at": row[10].isoformat(),
        "status": row[11],
    }


@app.post("/internal/sagas/{meal_id}/compensate")
def compensate_saga(
    meal_id: UUID,
    reason: str = "Unknown failure",
):
    with get_connection() as conn:
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
            raise HTTPException(
                status_code=404,
                detail="SAGA instance not found",
            )

        saga_id, current_status = saga

        if current_status != "PENDING":
            return {
                "saga_id": str(saga_id),
                "meal_id": str(meal_id),
                "status": current_status,
                "message": "No compensation required",
            }

        meal_updated = conn.execute(
            """
            UPDATE meal_entries
            SET status = 'CANCELLED'
            WHERE id = %s AND status = 'PENDING'
            RETURNING id
            """,
            (meal_id,),
        ).fetchone()

        if meal_updated is None:
            raise RuntimeError(
                f"Could not cancel pending meal {meal_id}"
            )

        conn.execute(
            """
            UPDATE saga_instances
            SET status = 'COMPENSATED',
                updated_at = NOW(),
                failure_reason = %s
            WHERE meal_id = %s AND status = 'PENDING'
            """,
            (reason[:1000], meal_id),
        )

    return {
        "saga_id": str(saga_id),
        "meal_id": str(meal_id),
        "status": "COMPENSATED",
        "reason": reason,
    }
