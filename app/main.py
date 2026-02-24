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
            try:
                conn   = get_connection()
                cursor = conn.cursor(dictionary=True)
                cursor.execute("""
                    SELECT id, store_id, product_id, date,
                           units_sold, price, discount,
                           is_holiday_promo, weather, received_at
                    FROM kafka_events
                    WHERE id > %s
                    ORDER BY id ASC
                    LIMIT 50
                """, (last_id,))
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
    """Price elasticity analysis."""
    return {"status": "stub", "product_id": product_id}


@app.get("/promotions/simulate")
def promotions_simulate(product_id: str, discount_pct: float):
    """Promotion effect simulation."""
    return {"status": "stub", "product_id": product_id, "discount_pct": discount_pct}