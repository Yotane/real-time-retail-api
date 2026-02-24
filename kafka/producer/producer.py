import os
import sys
import time
import json
import uuid
import logging
import httpx
import mysql.connector
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def get_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", 3306)),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE")
    )


def get_producer():
    return Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "client.id":         "retail-producer"
    })


def delivery_report(err, msg):
    if err:
        log.error(f"Delivery failed: {err}")


def fetch_all_dates(conn) -> list:
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT date FROM sales_facts ORDER BY date ASC")
    dates = [str(row[0]) for row in cursor.fetchall()]
    cursor.close()
    log.info(f"Found {len(dates)} unique dates")
    return dates


def fetch_day_rows(conn, day: str) -> list:
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT
            sf.date, sf.store_id, sf.product_id,
            sf.units_sold, sf.price, sf.discount,
            c.is_holiday_promo, c.weather_condition
        FROM sales_facts sf
        JOIN calendar c ON sf.date = c.date
        WHERE sf.date = %s
        ORDER BY sf.store_id, sf.product_id
    """, (day,))
    rows = cursor.fetchall()
    cursor.close()
    return rows


def send_day(producer, topic: str, rows: list, delay: float):
    log.info(f"Sending {len(rows)} events for day {rows[0]['date'] if rows else '?'}")
    
    for row in rows:
        event = {
            "event_id":        str(uuid.uuid4()),
            "store_id":        row["store_id"],
            "product_id":      row["product_id"],
            "date":            str(row["date"]),
            "units_sold":      row["units_sold"],
            "price":           float(row["price"])    if row["price"]    else None,
            "discount":        float(row["discount"]) if row["discount"] else None,
            "is_holiday_promo": int(row["is_holiday_promo"]),
            "weather":         row["weather_condition"],
            "day_total":       len(rows)
        }
        producer.produce(
            topic=topic,
            key=f"{row['store_id']}_{row['product_id']}",
            value=json.dumps(event),
            callback=delivery_report
        )
        producer.poll(0)
        time.sleep(delay)
    producer.flush()
    log.info(f"Day complete — {len(rows)} events sent")


def run():
    topic         = os.getenv("KAFKA_TOPIC_SALES", "sales-events")
    delay         = float(os.getenv("PRODUCER_DELAY_SECONDS", "0.05"))
    api_url       = os.getenv("API_URL", "http://api:8000")
    poll_interval = float(os.getenv("PRODUCER_POLL_SECONDS", "1.0"))

    log.info("Producer starting...")
    conn     = get_connection()
    producer = get_producer()
    dates    = fetch_all_dates(conn)

    # Retry registering dates with API until it succeeds
    log.info(f"Registering {len(dates)} dates with API...")
    while True:
        try:
            resp = httpx.post(
                f"{api_url}/simulation/init",
                json={"dates": dates},
                timeout=5
            )
            if resp.status_code == 200:
                log.info("Dates registered successfully with API")
                break
            else:
                log.warning(f"API returned {resp.status_code}, retrying...")
        except Exception as e:
            log.warning(f"Could not reach API: {e} — retrying in 2s...")
        time.sleep(2)

    current_day = None

    while True:
        try:
            resp        = httpx.get(f"{api_url}/simulation/status", timeout=5)
            state       = resp.json()
            wanted_day  = state.get("current_day")
            if wanted_day is None:
                current_day = None

            if wanted_day and wanted_day != current_day:
                current_day = wanted_day
                rows = fetch_day_rows(conn, current_day)
                if rows:
                    try:
                        httpx.post(f"{api_url}/simulation/day-total",
                                json={"total": len(rows)}, timeout=3)
                    except Exception:
                        pass
                    send_day(producer, topic, rows, delay)

        except Exception as e:
            log.warning(f"Polling error: {e}")

        time.sleep(poll_interval)



if __name__ == "__main__":
    run()