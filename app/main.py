import os
import json
import asyncio
import logging
import mysql.connector
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse, HTMLResponse
from pydantic import BaseModel
from typing import List
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = FastAPI(
    title="Retail Data API",
    description="Event-driven retail analytics with real-time Kafka streaming",
    version="0.1.0"
)

# Shared simulation state
from app.simulation import simulation


def get_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", 3306)),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE")
    )


# ─── Health ────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "service": "api"}


# ─── Simulation Control ────────────────────────────────────────────────────

class InitPayload(BaseModel):
    dates: List[str]


@app.post("/simulation/init")
def simulation_init(payload: InitPayload):
    """Called by producer on startup to register all available dates."""
    simulation.all_dates = payload.dates
    log.info(f"Simulation initialized with {len(payload.dates)} dates")
    return {"status": "ok", "total_days": len(payload.dates)}


@app.get("/simulation/status")
def simulation_status():
    """Producer polls this to know which day to send. Browser polls for progress."""
    return simulation.to_dict()


@app.post("/simulation/next-day")
def simulation_next_day():
    """User clicks Next Day — advances simulation by one date."""
    if not simulation.all_dates:
        raise HTTPException(status_code=400, detail="Simulation not initialized yet. Wait for producer to start.")
    if simulation.is_complete:
        raise HTTPException(status_code=400, detail="All days completed. No more data.")
    day = simulation.advance_day()
    return {"status": "ok", "current_day": day}


@app.post("/simulation/reset")
def simulation_reset():
    """Reset simulation back to day 0."""
    simulation.current_index = -1
    simulation.current_day   = None
    simulation.day_total      = 0
    simulation.day_received   = 0
    simulation.is_running     = False
    simulation.is_complete    = False
    return {"status": "reset"}

@app.post("/simulation/day-total")
def set_day_total(payload: dict):
    simulation.set_day_total(payload["total"])
    return {"status": "ok"}

# ─── Real-Time SSE ─────────────────────────────────────────────────────────

@app.get("/stream/live", response_class=HTMLResponse)
def stream_page():
    """The real-time browser experience — Day-by-Day simulation."""
    html_path = os.path.join(os.path.dirname(__file__), "stream_page.html")
    with open(html_path, "r", encoding="utf-8") as f:
        html_content = f.read()
    return HTMLResponse(content=html_content)


@app.get("/stream/events")
async def stream_events():
    """SSE endpoint — tails kafka_events table and pushes to browser."""

    async def event_generator():
        last_id = 0
        while True:

            if not simulation.current_day:
                await asyncio.sleep(0.1)
                continue

            try:
                conn   = get_connection()
                cursor = conn.cursor(dictionary=True)
                cursor.execute("""
                    SELECT id, store_id, product_id, date,
                        units_sold, price, discount,
                        is_holiday_promo, weather, received_at
                    FROM kafka_events
                    WHERE id > %s AND date = %s
                    ORDER BY id ASC
                    LIMIT 50
                """, (last_id, simulation.current_day))
                rows = cursor.fetchall()
                cursor.close()
                conn.close()

                for row in rows:
                    last_id = row["id"]
                    event = {}
                    for k, v in row.items():
                        if hasattr(v, 'isoformat'):      # date / datetime
                            event[k] = str(v)
                        elif hasattr(v, '__float__'):    # Decimal
                            event[k] = float(v)
                        else:
                            event[k] = v
                    event["day_total"] = simulation.day_total
                    yield f"data: {json.dumps(event)}\n\n"

            except Exception as e:
                log.warning(f"SSE DB error: {e}")

            await asyncio.sleep(0.1)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":               "no-cache",
            "X-Accel-Buffering":           "no",
            "Access-Control-Allow-Origin": "*"
        }
    )


# ─── Analytics Endpoints ──────────────────────────────────────────────────

@app.get("/sales/recent")
def sales_recent(limit: int = 20):
    conn   = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT store_id, product_id, date, units_sold,
               price, discount, is_holiday_promo, weather, received_at
        FROM kafka_events
        ORDER BY received_at DESC
        LIMIT %s
    """, (limit,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {"events": [dict(r) for r in rows]}


@app.get("/demand/predict")
def demand_predict(store_id: str, product_id: str):
    """Moving average demand forecast for a product/store."""
    return {"status": "stub", "store_id": store_id, "product_id": product_id}


@app.get("/price/sensitivity")
def price_sensitivity(product_id: str):
    """
    Price elasticity analysis.
    Calculates how much demand changes when price changes.
    Elasticity < -1 means demand is elastic (sensitive to price).
    Elasticity > -1 means demand is inelastic (not sensitive).
    """
    conn   = get_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT price, units_sold, discount, store_id
        FROM sales_facts
        WHERE product_id = %s AND price > 0
        ORDER BY date ASC
    """, (product_id,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for product={product_id}"
        )

    import pandas as pd
    import numpy as np

    df = pd.DataFrame(rows)
    df["price"]      = pd.to_numeric(df["price"],      errors="coerce")
    df["units_sold"] = pd.to_numeric(df["units_sold"], errors="coerce")
    df = df.dropna(subset=["price", "units_sold"])

    # Price buckets — group by rounded price, get avg units sold
    df["price_bucket"] = df["price"].round(0)
    grouped = df.groupby("price_bucket")["units_sold"].mean().reset_index()
    grouped = grouped.sort_values("price_bucket")

    # Elasticity = % change in quantity / % change in price
    if len(grouped) >= 2:
        pct_change_price    = grouped["price_bucket"].pct_change().dropna()
        pct_change_quantity = grouped["units_sold"].pct_change().dropna()
        valid = pct_change_price[pct_change_price != 0]
        if len(valid) > 0:
            elasticity = float((pct_change_quantity / pct_change_price).mean())
        else:
            elasticity = 0.0
    else:
        elasticity = 0.0

    # Interpretation
    if elasticity < -1:
        interpretation = "Elastic — customers are price sensitive. Lowering price increases revenue."
    elif -1 <= elasticity < 0:
        interpretation = "Inelastic — customers are not very price sensitive. Price increases have little demand impact."
    elif elasticity >= 0:
        interpretation = "Positive correlation — higher price correlates with higher sales (possible premium/luxury effect)."
    else:
        interpretation = "Insufficient price variation to determine elasticity."

    # Correlation coefficient
    correlation = float(df["price"].corr(df["units_sold"]))

    # Price range breakdown
    price_brackets = grouped.rename(columns={
        "price_bucket": "price_point",
        "units_sold":   "avg_units_sold"
    }).round(2).to_dict(orient="records")

    return {
        "product_id":       product_id,
        "total_records":    len(df),
        "price_range":      {"min": round(float(df["price"].min()), 2),
                             "max": round(float(df["price"].max()), 2),
                             "mean": round(float(df["price"].mean()), 2)},
        "avg_units_sold":   round(float(df["units_sold"].mean()), 2),
        "elasticity":       round(elasticity, 4),
        "correlation":      round(correlation, 4),
        "interpretation":   interpretation,
        "price_brackets":   price_brackets[:10]
    }

@app.get("/promotions/simulate")
def promotions_simulate(product_id: str, discount_pct: float):
    """Promotion effect simulation."""
    return {"status": "stub", "product_id": product_id, "discount_pct": discount_pct}