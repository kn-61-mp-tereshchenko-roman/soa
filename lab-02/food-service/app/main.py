from uuid import UUID, uuid4

import psycopg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

import os


app = FastAPI(title="Food Catalog Service")

DATABASE_URL = os.environ["DATABASE_URL"]


def get_connection():
    return psycopg.connect(DATABASE_URL)


@app.on_event("startup")
def initialize_database():
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS food_items (
                id UUID PRIMARY KEY,
                name TEXT NOT NULL,
                calories_per_100g DOUBLE PRECISION NOT NULL
                    CHECK (calories_per_100g >= 0),
                protein_per_100g DOUBLE PRECISION NOT NULL
                    CHECK (protein_per_100g >= 0),
                fat_per_100g DOUBLE PRECISION NOT NULL
                    CHECK (fat_per_100g >= 0),
                carbs_per_100g DOUBLE PRECISION NOT NULL
                    CHECK (carbs_per_100g >= 0)
            )
            """
        )


class FoodCreate(BaseModel):
    name: str = Field(min_length=1, max_length=150)
    calories_per_100g: float = Field(ge=0, le=1000)
    protein_per_100g: float = Field(ge=0, le=100)
    fat_per_100g: float = Field(ge=0, le=100)
    carbs_per_100g: float = Field(ge=0, le=100)


def food_to_dict(row):
    return {
        "id": str(row[0]),
        "name": row[1],
        "calories_per_100g": row[2],
        "protein_per_100g": row[3],
        "fat_per_100g": row[4],
        "carbs_per_100g": row[5],
    }


@app.get("/api/foods/health")
def health():
    return {
        "status": "ok",
        "service": "food-catalog-service",
    }


@app.post("/api/foods", status_code=201)
def create_food(data: FoodCreate):
    food_id = uuid4()

    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO food_items (
                id,
                name,
                calories_per_100g,
                protein_per_100g,
                fat_per_100g,
                carbs_per_100g
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                food_id,
                data.name,
                data.calories_per_100g,
                data.protein_per_100g,
                data.fat_per_100g,
                data.carbs_per_100g,
            ),
        )

    return {
        "id": str(food_id),
        **data.model_dump(),
    }


@app.get("/api/foods")
def list_foods():
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                id,
                name,
                calories_per_100g,
                protein_per_100g,
                fat_per_100g,
                carbs_per_100g
            FROM food_items
            ORDER BY name
            """
        ).fetchall()

    return [food_to_dict(row) for row in rows]


@app.get("/api/foods/{food_id}")
def get_food(food_id: UUID):
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT
                id,
                name,
                calories_per_100g,
                protein_per_100g,
                fat_per_100g,
                carbs_per_100g
            FROM food_items
            WHERE id = %s
            """,
            (food_id,),
        ).fetchone()

    if row is None:
        raise HTTPException(
            status_code=404,
            detail="Food item not found",
        )

    return food_to_dict(row)
