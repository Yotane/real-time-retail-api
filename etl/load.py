import os
import sys
import logging
import pandas as pd
import mysql.connector
import time
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

def get_connection_with_retry(retries=10, delay=5):
    for attempt in range(retries):
        try:
            conn = mysql.connector.connect(
                host=os.getenv("MYSQL_HOST", "localhost"),
                port=int(os.getenv("MYSQL_PORT", 3306)),
                user=os.getenv("MYSQL_USER"),
                password=os.getenv("MYSQL_PASSWORD"),
                database=os.getenv("MYSQL_DATABASE")
            )
            log.info("Connected to MySQL successfully")
            return conn
        except mysql.connector.Error as err:
            log.warning(f"Attempt {attempt + 1}/{retries} failed: {err}")
            if attempt < retries - 1:
                time.sleep(delay)
    raise Exception("Could not connect to MySQL after all retries")

def load_csv(path: str) -> pd.DataFrame:
    log.info(f"Loading CSV from {path}")
    df = pd.read_csv(path)
    df.columns = [c.strip().lower().replace(" ", "_").replace("/", "_") for c in df.columns]
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["holiday_promotion"] = df["holiday_promotion"].astype(int)
    log.info(f"Loaded {len(df):,} rows, {df.shape[1]} columns")
    return df


def run_schema(conn) -> None:
    log.info("Running schema creation...")
    with open("etl/schema.sql") as f:
        sql = f.read()
    cursor = conn.cursor()
    for statement in sql.split(";"):
        stmt = statement.strip()
        if stmt:
            cursor.execute(stmt)
    conn.commit()
    cursor.close()
    log.info("Schema ready.")


def load_stores(df: pd.DataFrame, conn) -> None:
    stores = (
        df[["store_id", "region"]]
        .drop_duplicates(subset=["store_id"])
        .reset_index(drop=True)
    )
    log.info(f"Loading {len(stores)} stores...")
    cursor = conn.cursor()
    data = [tuple(row) for row in stores.itertuples(index=False)]
    cursor.executemany("""
        INSERT INTO stores (store_id, region)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE region = VALUES(region)
    """, data)
    conn.commit()
    cursor.close()
    log.info("Stores loaded.")


def load_products(df: pd.DataFrame, conn) -> None:
    products = (
        df[["product_id", "category"]]
        .drop_duplicates(subset=["product_id"])
        .reset_index(drop=True)
    )
    log.info(f"Loading {len(products)} products...")
    cursor = conn.cursor()
    data = [tuple(row) for row in products.itertuples(index=False)]
    cursor.executemany("""
        INSERT INTO products (product_id, category)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE category = VALUES(category)
    """, data)
    conn.commit()
    cursor.close()
    log.info("Products loaded.")


def load_calendar(df: pd.DataFrame, conn) -> None:
    calendar = (
        df[["date", "weather_condition", "holiday_promotion", "seasonality"]]
        .drop_duplicates(subset=["date"])
        .reset_index(drop=True)
    )
    log.info(f"Loading {len(calendar)} calendar entries...")
    cursor = conn.cursor()
    data = [tuple(row) for row in calendar.itertuples(index=False)]
    cursor.executemany("""
        INSERT INTO calendar (date, weather_condition, is_holiday_promo, seasonality)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            weather_condition = VALUES(weather_condition),
            is_holiday_promo  = VALUES(is_holiday_promo),
            seasonality       = VALUES(seasonality)
    """, data)
    conn.commit()
    cursor.close()
    log.info("Calendar loaded.")


def load_sales_facts(df: pd.DataFrame, conn) -> None:
    log.info(f"Loading {len(df):,} sales fact rows...")
    facts = df[[
        "date", "store_id", "product_id",
        "inventory_level", "units_sold", "units_ordered",
        "demand_forecast", "price", "discount", "competitor_pricing"
    ]].copy()
    cursor = conn.cursor()
    data = [tuple(row) for row in facts.itertuples(index=False)]
    chunk_size = 1000
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i + chunk_size]
        cursor.executemany("""
            INSERT INTO sales_facts (
                date, store_id, product_id,
                inventory_level, units_sold, units_ordered,
                demand_forecast, price, discount, competitor_pricing
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                inventory_level    = VALUES(inventory_level),
                units_sold         = VALUES(units_sold),
                units_ordered      = VALUES(units_ordered),
                demand_forecast    = VALUES(demand_forecast),
                price              = VALUES(price),
                discount           = VALUES(discount),
                competitor_pricing = VALUES(competitor_pricing)
        """, chunk)
        conn.commit()
        log.info(f"Inserted rows {i} to {i + len(chunk)}")
    cursor.close()
    log.info("Sales facts loaded.")


def run():
    csv_path = os.getenv("CSV_PATH", "data/raw/retail_store_inventory.csv")
    if not os.path.exists(csv_path):
        log.error(f"CSV not found at {csv_path}")
        sys.exit(1)

    conn = None
    cursor = None
    try:
        conn = get_connection_with_retry(retries=10, delay=5)
        run_schema(conn)
        df = load_csv(csv_path)
        load_stores(df, conn)
        load_products(df, conn)
        load_calendar(df, conn)
        load_sales_facts(df, conn)
        log.info("ETL complete.")
    except mysql.connector.Error as err:
        log.error(f"MySQL error: {err}")
        if conn:
            conn.rollback()
        sys.exit(1)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    run()