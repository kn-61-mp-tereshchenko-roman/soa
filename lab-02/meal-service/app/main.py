import os
from datetime import datetime, timezone
from uuid import UUID, uuid4

import httpx
import psycopg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.messaging import publish_meal_created


app = FastAPI(title="Meal Service")

DATABASE_URL = os.environ["DATABASE_URL"]
PROFILE_SERVICE_URL = os.environ["PROFILE_SERVICE_URL"]
FOOD_SERVICE_URL = os.environ["FOOD_SERVICE_URL"]

# Identify the container instance handling the request.
# Docker normally provides a unique HOSTNAME for each container.
INSTANCE_ID = os.getenv("HOSTNAME", str(uuid4()))


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
                consumed_at TIMESTAMPTZ NOT NULL
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

    profile = request_remote_json(
        f"{PROFILE_SERVICE_URL}/api/profiles/{data.user_id}",
        "Profile Service is unavailable",
    )

    food = request_remote_json(
        f"{FOOD_SERVICE_URL}/api/foods/{data.food_id}",
        "Food Catalog Service is unavailable",
    )

    factor = data.quantity_grams / 100.0

    calories = round(
        food["calories_per_100g"] * factor,
        2,
    )

    protein = round(
        food["protein_per_100g"] * factor,
        2,
    )

    fat = round(
        food["fat_per_100g"] * factor,
        2,
    )

    carbs = round(
        food["carbs_per_100g"] * factor,
        2,
    )

    meal_id = uuid4()
    consumed_at = datetime.now(timezone.utc)

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
    }

    try:
        with get_connection() as conn:
            conn.execute(
                """
                INSERT INTO meal_entries (
                    id,
                    user_id,
                    food_id,
                    food_name,
                    meal_type,
                    quantity_grams,
                    calories,
                    protein,
                    fat,
                    carbs,
                    consumed_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
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

    except psycopg.Error as exc:
        raise HTTPException(
            status_code=500,
            detail="Could not save meal entry",
        ) from exc

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

    try:
        event = publish_meal_created(event_data)

    except Exception as exc:
        # The meal has already been committed to PostgreSQL.
        # Do not delete it merely because event publication failed.
        # A transactional outbox should retry publication reliably.
        raise HTTPException(
            status_code=503,
            detail=(
                "Meal was saved, but event publication failed. "
                "The record may exist; check its ID before retrying."
            ),
        ) from exc

    return {
        **record,
        "event_id": event["event_id"],
        "message": "Meal recorded and event published",
    }


@app.get("/api/meals")
def list_meals(user_id: UUID):
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                id,
                user_id,
                food_id,
                food_name,
                meal_type,
                quantity_grams,
                calories,
                protein,
                fat,
                carbs,
                consumed_at
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
        }
        for row in rows
    ]


@app.get("/api/meals/{meal_id}")
def get_meal(meal_id: UUID):
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT
                id,
                user_id,
                food_id,
                food_name,
                meal_type,
                quantity_grams,
                calories,
                protein,
                fat,
                carbs,
                consumed_at
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
    }
