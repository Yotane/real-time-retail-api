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
    """Most recent Kafka-ingested events."""
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
def demand_predict(store_id: str, product_id: str, days: int = 7, day: int = None):
    """
    Demand forecast using moving average on historical sales_facts.
    Use 'day' (1-731) to scope analysis up to that simulation day.
    Defaults to current simulation day if running, otherwise all history.
    """

    if day is not None:
        conn   = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT date FROM sales_facts
            ORDER BY date ASC LIMIT 1 OFFSET %s
        """, (day - 1,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if not row:
            raise HTTPException(status_code=400, detail=f"day {day} is out of range")
        cutoff = str(row[0])
    elif simulation.current_day:
        cutoff = simulation.current_day
    else:
        cutoff = None

    conn   = get_connection()
    cursor = conn.cursor(dictionary=True)

    if cutoff:
        cursor.execute("""
            SELECT date, units_sold
            FROM sales_facts
            WHERE store_id = %s AND product_id = %s AND date <= %s
            ORDER BY date ASC
        """, (store_id, product_id, cutoff))
    else:
        cursor.execute("""
            SELECT date, units_sold
            FROM sales_facts
            WHERE store_id = %s AND product_id = %s
            ORDER BY date ASC
        """, (store_id, product_id))

    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail=f"No data found for store={store_id} product={product_id}")

    import pandas as pd
    df = pd.DataFrame(rows)
    df["date"]       = pd.to_datetime(df["date"])
    df["units_sold"] = pd.to_numeric(df["units_sold"], errors="coerce").fillna(0)
    df = df.sort_values("date")

    window   = min(days, len(df))
    ma       = df["units_sold"].rolling(window=window, min_periods=1).mean()
    forecast = round(float(ma.iloc[-1]), 2)

    if len(df) >= 14:
        recent_avg   = df["units_sold"].iloc[-7:].mean()
        previous_avg = df["units_sold"].iloc[-14:-7].mean()
        trend_pct    = round(((recent_avg - previous_avg) / (previous_avg + 1e-9)) * 100, 1)
        trend        = "up" if trend_pct > 2 else "down" if trend_pct < -2 else "stable"
    else:
        trend_pct = 0.0
        trend     = "insufficient data"

    forward = [{"day": i + 1, "forecast_units": forecast} for i in range(7)]

    return {
        "store_id":           store_id,
        "product_id":         product_id,
        "analysis_up_to_day": cutoff or "all",
        "total_history_days": len(df),
        "moving_avg_window":  window,
        "forecast_next_day":  forecast,
        "trend":              trend,
        "trend_pct":          trend_pct,
        "7_day_forward":      forward,
        "last_5_actuals":     df[["date", "units_sold"]].tail(5).assign(
            date=lambda x: x["date"].dt.strftime("%Y-%m-%d")
        ).to_dict(orient="records")
    }


@app.get("/price/sensitivity")
def price_sensitivity(product_id: str, day: int = None):
    """
    Price elasticity analysis.
    Use 'day' (1-731) to scope analysis up to that simulation day.
    Defaults to current simulation day if running, otherwise all history.
    """
    if day is not None:
        conn   = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT date FROM sales_facts
            ORDER BY date ASC LIMIT 1 OFFSET %s
        """, (day - 1,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if not row:
            raise HTTPException(status_code=400, detail=f"day {day} is out of range")
        cutoff = str(row[0])
    elif simulation.current_day:
        cutoff = simulation.current_day
    else:
        cutoff = None

    conn   = get_connection()
    cursor = conn.cursor(dictionary=True)

    if cutoff:
        cursor.execute("""
            SELECT price, units_sold, discount, store_id
            FROM sales_facts
            WHERE product_id = %s AND price > 0 AND date <= %s
            ORDER BY date ASC
        """, (product_id, cutoff))
    else:
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
        raise HTTPException(status_code=404, detail=f"No data found for product={product_id}")

    import pandas as pd
    import numpy as np

    df = pd.DataFrame(rows)
    df["price"]      = pd.to_numeric(df["price"],      errors="coerce")
    df["units_sold"] = pd.to_numeric(df["units_sold"], errors="coerce")
    df = df.dropna(subset=["price", "units_sold"])

    df["price_bucket"] = df["price"].round(0)
    grouped = df.groupby("price_bucket")["units_sold"].mean().reset_index()
    grouped = grouped.sort_values("price_bucket")

    if len(grouped) >= 2:
        pct_change_price    = grouped["price_bucket"].pct_change().dropna()
        pct_change_quantity = grouped["units_sold"].pct_change().dropna()
        valid = pct_change_price[pct_change_price != 0]
        elasticity = float((pct_change_quantity / pct_change_price).mean()) if len(valid) > 0 else 0.0
    else:
        elasticity = 0.0

    if elasticity < -1:
        interpretation = "Elastic — customers are price sensitive. Lowering price increases revenue."
    elif -1 <= elasticity < 0:
        interpretation = "Inelastic — customers are not very price sensitive. Price increases have little demand impact."
    elif elasticity >= 0:
        interpretation = "Positive correlation — higher price correlates with higher sales (possible premium/luxury effect)."
    else:
        interpretation = "Insufficient price variation to determine elasticity."

    correlation    = float(df["price"].corr(df["units_sold"]))
    price_brackets = grouped.rename(columns={
        "price_bucket": "price_point",
        "units_sold":   "avg_units_sold"
    }).round(2).to_dict(orient="records")

    return {
        "product_id":       product_id,
        "analysis_up_to_day": cutoff or "all",
        "total_records":    len(df),
        "price_range":      {
            "min":  round(float(df["price"].min()), 2),
            "max":  round(float(df["price"].max()), 2),
            "mean": round(float(df["price"].mean()), 2)
        },
        "avg_units_sold":   round(float(df["units_sold"].mean()), 2),
        "elasticity":       round(elasticity, 4),
        "correlation":      round(correlation, 4),
        "interpretation":   interpretation,
        "price_brackets":   price_brackets[:10]
    }


@app.get("/promotions/simulate")
def promotions_simulate(product_id: str, discount_pct: float, day: int = None):
    """
    Simulate the effect of a promotion/discount on a product.
    Use 'day' (1-731) to scope analysis up to that simulation day.
    Defaults to current simulation day if running, otherwise all history.
    """
    if day is not None:
        conn   = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT date FROM sales_facts
            ORDER BY date ASC LIMIT 1 OFFSET %s
        """, (day - 1,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if not row:
            raise HTTPException(status_code=400, detail=f"day {day} is out of range")
        cutoff = str(row[0])
    elif simulation.current_day:
        cutoff = simulation.current_day
    else:
        cutoff = None

    conn   = get_connection()
    cursor = conn.cursor(dictionary=True)

    if cutoff:
        cursor.execute("""
            SELECT sf.units_sold, sf.price, sf.discount,
                   c.is_holiday_promo
            FROM sales_facts sf
            JOIN calendar c ON sf.date = c.date
            WHERE sf.product_id = %s AND sf.date <= %s
        """, (product_id, cutoff))
    else:
        cursor.execute("""
            SELECT sf.units_sold, sf.price, sf.discount,
                   c.is_holiday_promo
            FROM sales_facts sf
            JOIN calendar c ON sf.date = c.date
            WHERE sf.product_id = %s
        """, (product_id,))

    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail=f"No data found for product={product_id}")

    import pandas as pd

    df = pd.DataFrame(rows)
    df["units_sold"]       = pd.to_numeric(df["units_sold"],       errors="coerce")
    df["price"]            = pd.to_numeric(df["price"],            errors="coerce")
    df["discount"]         = pd.to_numeric(df["discount"],         errors="coerce")
    df["is_holiday_promo"] = df["is_holiday_promo"].astype(int)

    promo     = df[df["is_holiday_promo"] == 1]["units_sold"]
    non_promo = df[df["is_holiday_promo"] == 0]["units_sold"]

    avg_promo     = float(promo.mean())     if len(promo)     > 0 else 0.0
    avg_non_promo = float(non_promo.mean()) if len(non_promo) > 0 else 0.0

    historical_uplift_pct = round(
        ((avg_promo - avg_non_promo) / avg_non_promo) * 100, 1
    ) if avg_non_promo > 0 else 0.0

    discount_groups = df.groupby("discount")["units_sold"].mean().reset_index()
    discount_groups = discount_groups.sort_values("discount")
    discount_effect = discount_groups.rename(columns={
        "discount":   "discount_pct",
        "units_sold": "avg_units_sold"
    }).round(2).to_dict(orient="records")

    df_with_discount = df[df["discount"] > 0]
    if len(df_with_discount) > 0:
        closest_tier    = discount_groups.iloc[
            (discount_groups["discount"] - discount_pct).abs().argmin()
        ]
        projected_units = round(float(closest_tier["units_sold"]), 2)
    else:
        scale           = discount_pct / 10.0
        projected_units = round(avg_non_promo * (1 + (historical_uplift_pct / 100) * scale), 2)

    projected_uplift_pct = round(
        ((projected_units - avg_non_promo) / (avg_non_promo + 1e-9)) * 100, 1
    ) if avg_non_promo > 0 else 0.0

    return {
        "product_id":              product_id,
        "analysis_up_to_day":      cutoff or "all",
        "simulated_discount_pct":  discount_pct,
        "baseline_avg_units":      round(avg_non_promo, 2),
        "promo_avg_units":         round(avg_promo, 2),
        "historical_uplift_pct":   historical_uplift_pct,
        "projected_units":         projected_units,
        "projected_uplift_pct":    projected_uplift_pct,
        "total_promo_days":        len(promo),
        "total_non_promo_days":    len(non_promo),
        "discount_effect_by_tier": discount_effect,
        "recommendation": (
            f"A {discount_pct}% discount is projected to increase daily sales "
            f"from {round(avg_non_promo, 1)} to {projected_units} units "
            f"({projected_uplift_pct:+.1f}% uplift)."
        )
    }


@app.get("/metrics")
def metrics():
    """
    System health and performance metrics.
    Kafka consumer lag, ingestion rate, per-store totals.
    """
    from kafka.consumer.consumer import latest_events

    conn   = get_connection()
    cursor = conn.cursor(dictionary=True)

    # Total events ingested
    cursor.execute("SELECT COUNT(*) as total FROM kafka_events")
    total_ingested = cursor.fetchone()["total"]

    # Events in last 60 seconds
    cursor.execute("""
        SELECT COUNT(*) as recent
        FROM kafka_events
        WHERE received_at >= NOW() - INTERVAL 60 SECOND
    """)
    recent_count = cursor.fetchone()["recent"]

    # Per-store totals
    cursor.execute("""
        SELECT store_id,
               COUNT(*)        as total_events,
               SUM(units_sold) as total_units,
               SUM(units_sold * price) as total_revenue
        FROM kafka_events
        GROUP BY store_id
        ORDER BY total_revenue DESC
    """)
    store_totals = cursor.fetchall()

    # Top 5 products by units
    cursor.execute("""
        SELECT product_id,
               SUM(units_sold) as total_units
        FROM kafka_events
        GROUP BY product_id
        ORDER BY total_units DESC
        LIMIT 5
    """)
    top_products = cursor.fetchall()

    cursor.close()
    conn.close()

    return {
        "kafka": {
            "in_memory_buffer":   len(latest_events),
            "buffer_capacity":    500,
            "buffer_pct_full":    round(len(latest_events) / 500 * 100, 1)
        },
        "ingestion": {
            "total_events_stored":    total_ingested,
            "events_last_60s":        recent_count,
            "events_per_minute":      recent_count
        },
        "simulation": simulation.to_dict(),
        "store_totals":  [dict(r) for r in store_totals],
        "top_5_products": [dict(r) for r in top_products]
    }

# ─── Optuna Optimization Endpoints ───────────────────────────────────────

@app.post("/optimize/demand")
def optimize_demand(store_id: str, product_id: str, n_trials: int = 20, day: int = None):
    """
    Run Optuna to find best forecasting parameters,
    then immediately apply them to produce an optimized demand forecast.
    Use 'day' (1-731) to scope analysis up to that simulation day.
    Defaults to current simulation day if running, otherwise all history.
    """
    from app.optimizer import run_optimization

    if n_trials < 5 or n_trials > 50:
        raise HTTPException(status_code=400, detail="n_trials must be between 5 and 50")

    if day is not None:
        conn   = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT date FROM sales_facts
            ORDER BY date ASC LIMIT 1 OFFSET %s
        """, (day - 1,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if not row:
            raise HTTPException(status_code=400, detail=f"day {day} is out of range")
        cutoff = str(row[0])
    elif simulation.current_day:
        cutoff = simulation.current_day
    else:
        cutoff = None

    result = run_optimization(store_id, product_id, n_trials, cutoff)

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    return result


@app.get("/optimize/history")
def optimize_history(product_id: str = None, store_id: str = None, limit: int = 200):
    """All past Optuna trials stored in MySQL."""
    conn   = get_connection()
    cursor = conn.cursor(dictionary=True)

    if product_id and store_id:
        cursor.execute("""
            SELECT * FROM optuna_trials
            WHERE product_id = %s AND store_id = %s
            ORDER BY completed_at DESC LIMIT %s
        """, (product_id, store_id, limit))
    elif product_id:
        cursor.execute("""
            SELECT * FROM optuna_trials
            WHERE product_id = %s
            ORDER BY completed_at DESC LIMIT %s
        """, (product_id, limit))
    else:
        cursor.execute("""
            SELECT * FROM optuna_trials
            ORDER BY completed_at DESC LIMIT %s
        """, (limit,))

    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    if not rows:
        return {"trials": [], "message": "No runs yet. POST to /optimize/demand first."}

    def serialize(row):
        result = {}
        for k, v in row.items():
            if hasattr(v, 'isoformat'):
                result[k] = str(v)
            elif hasattr(v, '__float__'):
                result[k] = float(v)
            else:
                result[k] = v
        return result

    serialized = [serialize(r) for r in rows]
    best = min(serialized, key=lambda r: float(r["rmse"] or 999))

    return {
        "total_trials": len(serialized),
        "best_trial": {
            "rmse":         best["rmse"],
            "window":       best["_window"],
            "min_periods":  best["min_periods"],
            "trend_window": best["trend_window"]
        },
        "trials": serialized
    }