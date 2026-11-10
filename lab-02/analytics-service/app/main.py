import os
from datetime import date
from uuid import UUID

import psycopg
from fastapi import FastAPI, HTTPException, Query


app = FastAPI(title="Nutrition Analytics Service")

DATABASE_URL = os.environ["DATABASE_URL"]


def get_connection():
    return psycopg.connect(DATABASE_URL)


@app.on_event("startup")
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


@app.get("/api/analytics/health")
def health():
    return {
        "status": "ok",
        "service": "nutrition-analytics-service",
    }


@app.get("/api/analytics/daily")
def get_daily_summary(
    user_id: UUID,
    summary_date: date = Query(
        default_factory=date.today,
    ),
):
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT
                user_id,
                summary_date,
                total_calories,
                total_protein,
                total_fat,
                total_carbs
            FROM daily_nutrition_summary
            WHERE user_id = %s
              AND summary_date = %s
            """,
            (user_id, summary_date),
        ).fetchone()

    if row is None:
        return {
            "user_id": str(user_id),
            "date": summary_date.isoformat(),
            "total_calories": 0,
            "total_protein": 0,
            "total_fat": 0,
            "total_carbs": 0,
            "message": "No nutrition events processed for this date",
        }

    return {
        "user_id": str(row[0]),
        "date": row[1].isoformat(),
        "total_calories": round(row[2], 2),
        "total_protein": round(row[3], 2),
        "total_fat": round(row[4], 2),
        "total_carbs": round(row[5], 2),
    }
