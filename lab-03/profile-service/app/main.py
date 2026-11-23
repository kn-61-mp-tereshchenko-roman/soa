import os
from uuid import uuid4

import psycopg
from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from app.security import require_scope


app = FastAPI(title="Profile Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://cad.kpi.ua"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)

DATABASE_URL = os.environ["DATABASE_URL"]


def get_connection():
    return psycopg.connect(DATABASE_URL)


@app.on_event("startup")
def initialize_database():
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_profiles (
                id UUID PRIMARY KEY,
                name TEXT NOT NULL,
                daily_calorie_goal INTEGER NOT NULL
                    CHECK (daily_calorie_goal > 0)
            )
            """
        )


class ProfileCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    daily_calorie_goal: int = Field(gt=0, le=10000)


@app.get("/api/profiles/health")
def health():
    return {
        "status": "ok",
        "service": "profile-service",
    }


@app.post("/api/profiles", status_code=201)
def create_profile(
    data: ProfileCreate,
    token: dict = Depends(require_scope("profiles:write")),
):
    profile_id = uuid4()

    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO user_profiles (
                id, name, daily_calorie_goal
            )
            VALUES (%s, %s, %s)
            """,
            (
                profile_id,
                data.name,
                data.daily_calorie_goal,
            ),
        )

    return {
        "id": str(profile_id),
        "name": data.name,
        "daily_calorie_goal": data.daily_calorie_goal,
    }


@app.get("/api/profiles/{profile_id}")
def get_profile(
    profile_id: str,
    token: dict = Depends(require_scope("profiles:read")),
):
    try:
        with get_connection() as conn:
            row = conn.execute(
                """
                SELECT id, name, daily_calorie_goal
                FROM user_profiles
                WHERE id = %s
                """,
                (profile_id,),
            ).fetchone()

    except psycopg.Error as exc:
        raise HTTPException(
            status_code=500,
            detail="Database operation failed",
        ) from exc

    if row is None:
        raise HTTPException(
            status_code=404,
            detail="Profile not found",
        )

    return {
        "id": str(row[0]),
        "name": row[1],
        "daily_calorie_goal": row[2],
    }