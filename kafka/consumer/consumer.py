import os
import json
import logging
import mysql.connector
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

latest_events  = []
day_summary    = {}
MAX_EVENTS     = 500


def get_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", 3306)),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE")
    )


def get_consumer():
    return Consumer({
        "bootstrap.servers":  os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "group.id":           os.getenv("KAFKA_GROUP_ID", "retail-consumer-group"),
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True
    })


def insert_event(conn, event: dict):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO kafka_events (
            event_id, store_id, product_id, date,
            units_sold, price, discount,
            is_holiday_promo, weather
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        event["event_id"],
        event["store_id"],
        event["product_id"],
        event["date"],
        event["units_sold"],
        event["price"],
        event["discount"],
        event["is_holiday_promo"],
        event["weather"]
    ))
    conn.commit()
    cursor.close()


def update_day_summary(event: dict):
    """Build running totals for the current day."""
    date = event["date"]
    if date not in day_summary:
        day_summary[date] = {
            "date":          date,
            "total_events":  0,
            "total_units":   0,
            "total_revenue": 0.0,
            "promo_events":  0,
            "top_products":  {}
        }
    s = day_summary[date]
    s["total_events"]  += 1
    s["total_units"]   += event["units_sold"] or 0
    revenue = (event["units_sold"] or 0) * (event["price"] or 0)
    s["total_revenue"] += revenue
    if event["is_holiday_promo"]:
        s["promo_events"] += 1
    pid = event["product_id"]
    s["top_products"][pid] = s["top_products"].get(pid, 0) + (event["units_sold"] or 0)


def run():
    topic    = os.getenv("KAFKA_TOPIC_SALES", "sales-events")
    consumer = get_consumer()
    consumer.subscribe([topic])
    conn     = get_connection()

    log.info(f"Consumer subscribed to: {topic}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error(f"Kafka error: {msg.error()}")
                continue

            event = json.loads(msg.value().decode("utf-8"))
            log.info(f"Received: {event['store_id']} {event['product_id']} {event['units_sold']} units")

            insert_event(conn, event)
            update_day_summary(event)

            latest_events.append(event)
            if len(latest_events) > MAX_EVENTS:
                latest_events.pop(0)

    except KeyboardInterrupt:
        log.info("Consumer shutting down...")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    run()