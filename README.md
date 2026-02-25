# Real-Time Retail API with Optuna Forecasting
**Data Engineering portfolio project demonstrating event-driven architecture, real-time streaming, and ML-powered demand forecasting.**

A Kafka-backed retail analytics pipeline with a day-by-day time simulation. Step through 731 days of retail history clicking "Next Day" in a browser where each click triggers a Kafka event burst, streams transactions live through the pipeline, and updates real-time analytics.

Dataset sourced from Kaggle — ["Retail Store Inventory Forecasting Dataset"](https://www.kaggle.com/datasets/anirudhchauhan/retail-store-inventory-forecasting-dataset) by anirudhchauhan. 73,100 rows covering 5 stores, 20 products, and 731 days (2022–2024).

## Architecture

```text
CSV (Kaggle) ──> ETL (Python) ──> MySQL Database
                                        │
                              Kafka Producer ──> Kafka (KRaft)
                                                      │
                                              Kafka Consumer
                                                      │
                                              FastAPI (SSE) ──> Browser UI
                                                      │
                                              Optuna Optimizer
```

Each "Next Day" click:
1. FastAPI advances `SimulationState` (in-memory, thread-safe)
2. Producer detects new day via polling → fetches rows from MySQL → publishes to Kafka
3. Consumer reads Kafka → writes to `kafka_events` MySQL table
4. SSE endpoint tails `kafka_events` by `id` → pushes events to browser in real time

## Tech Stack

- **Language:** Python 3.11
- **API:** FastAPI + Uvicorn
- **Real-time browser:** Server-Sent Events (SSE) — browser-native, no WebSockets
- **Database:** MySQL 8.0, raw `mysql-connector-python`
- **Streaming:** Kafka 7.6.0 (Confluent) KRaft mode
- **Optimization:** Optuna 3.6.1 (TPE sampler, hyperparameter search)
- **Containers:** Docker + Docker Compose (6 services)
- **Data:** pandas, numpy

## Prerequisites

- Docker Desktop
- Docker Compose


## Installation

```bash
# Clone repository
git clone https://github.com/Yotane/real-time-retail-api.git
cd real-time-retail-api

# Configure environment
cp .env.example .env
# Edit .env with your preferred credentials
```

Download the dataset from [Kaggle](https://www.kaggle.com/datasets/anirudhchauhan/retail-store-inventory-forecasting-dataset) and place it at:
```
data/raw/retail_store_inventory.csv
```

## Running the Project

```bash
# First run — builds images, loads data, starts all services
docker compose up --build

# Subsequent runs (data already loaded)
docker compose up

# Full clean reset (wipes database volume)
docker compose down -v
docker compose up --build
```

Once running, open the browser UI:
```
http://localhost:8000/stream/live
```

Click **▶ Next Day** to begin streaming. Each click sends that day's transactions through Kafka in real time.

API docs (Swagger UI):
```
http://localhost:8000/docs
```

## Testing

Integration tests run against live containers using `httpx`. Docker Compose must be running before executing tests.
```bash

# Run all tests
pytest tests/test_api.py -v

# Run a specific class
pytest tests/test_api.py::TestOptimizeDemand -v
```

28 tests across 6 classes:

| Class | Coverage |
|-------|----------|
| `TestHealth` | `/health` endpoint |
| `TestSimulationStatus` | Status fields, types, progress range |
| `TestSimulationReset` | Reset clears state correctly |
| `TestSimulationNextDay` | Day advancement, date format, index increment |
| `TestSalesRecent` | Event shape, limit param, default limit |
| `TestOptimizeHistory` | Trial shape, filters, RMSE validity |
| `TestOptimizeDemand` | Forecast shape, trial count, trend values, validation errors |

Note: `TestSimulationNextDay` tests auto-skip if the producer hasn't registered dates yet (`total_days == 0`). Run `docker compose up` and wait for the producer to start before running the full suite.

## Project Structure

```
real-time-retail-api/
├── app/
│   ├── main.py           # FastAPI endpoints, SSE stream, simulation control
│   ├── simulation.py     # SimulationState dataclass with threading lock
│   └── optimizer.py      # Optuna demand forecasting optimizer
├── etl/
│   ├── schema.sql        # MySQL schema (5 tables + indexes)
│   └── load.py           # CSV → MySQL ETL with retry logic
├── kafka/
│   ├── producer/
│   │   └── producer.py   # Polls API for current day, sends rows to Kafka
│   └── consumer/
│       └── consumer.py   # Reads Kafka, writes to MySQL
├── data/raw/             # Place dataset CSV here (gitignored)
├── docker-compose.yml    # All 6 services
├── Dockerfile.api
├── Dockerfile.etl
├── Dockerfile.producer
├── Dockerfile.consumer
├── requirements.txt
└── .env.example
```

## Docker Services

| Service | Description | Port |
|---------|-------------|------|
| `mysql` | MySQL 8.0, persistent named volume | 3307 |
| `etl` | One-shot CSV loader, exits after completion | — |
| `kafka` | Confluent Kafka 7.6.0 KRaft mode | 9092 |
| `producer` | Polls simulation API, publishes daily events | — |
| `consumer` | Reads Kafka, writes to `kafka_events` | — |
| `api` | FastAPI — simulation control, SSE, analytics | 8000 |

Startup order: `mysql (healthy)` → `etl (completed)` → `kafka (healthy)` → `producer + consumer + api`

## Database Schema

```
stores        → store_id (PK), region
products      → product_id (PK), category
calendar      → date (PK), weather_condition, is_holiday_promo, seasonality
sales_facts   → id, date, store_id, product_id, inventory_level, units_sold,
                units_ordered, demand_forecast, price, discount, competitor_pricing
                UNIQUE (date, store_id, product_id)
kafka_events  → id, event_id (UNIQUE), store_id, product_id, date,
                units_sold, price, discount, is_holiday_promo, weather, received_at
optuna_trials → id, study_name, trial_number, store_id, product_id,
                _window, min_periods, trend_window, rmse, completed_at
```

## API Endpoints

### Simulation

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/stream/live` | Browser UI — day-by-day simulation |
| `GET` | `/stream/events` | SSE endpoint pushing live Kafka events |
| `POST` | `/simulation/next-day` | Advance simulation by one day |
| `POST` | `/simulation/reset` | Reset to day 0 |
| `GET` | `/simulation/status` | Current day, progress, totals |
| `POST` | `/simulation/init` | Register dates (called by producer on startup) |

### Analytics

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/sales/recent` | Last N events from kafka_events |
| `GET` | `/demand/predict` | Moving average demand forecast |
| `GET` | `/price/sensitivity` | Price elasticity analysis |
| `GET` | `/promotions/simulate` | Promotion effect simulation |

### Optuna Optimization

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/optimize/demand` | Run Optuna → find best forecast params → return optimized forecast |
| `GET` | `/optimize/history` | Query past optimization runs from MySQL |

## Demand Forecasting with Optuna

`POST /optimize/demand?store_id=S001&product_id=P0001&n_trials=20`

Runs Bayesian hyperparameter search (TPE sampler) over:
- `_window` — moving average lookback window (3–30 days)
- `min_periods` — minimum data points required (1–7)
- `trend_window` — trend detection window (5–21 days)

Evaluates each combination on a held-out test set (last 20% of history) using RMSE. Returns the best parameters and immediately applies them to produce a 7-day forward forecast.

**Example response (abbreviated):**
```json
{
  "store_id": "S001",
  "product_id": "P0001",
  "forecast": {
    "forecast_next_day": 66,
    "trend": "down",
    "trend_pct": -55.5,
    "7_day_forward": [{"day": 1, "forecast_units": 66}, "..."],
    "last_5_actuals": ["..."]
  },
  "optimization": {
    "best_rmse": 109.68,
    "improvement_pct": 3.3,
    "best_params": {"window": 4, "min_periods": 7, "trend_window": 15},
    "top_5_trials": ["..."]
  }
}
```

Different store/product combinations converge to different optimal windows — S001/P0001 performs best with a short 4-day window (reactive to recent spikes), while S003/P0001 converges to 22 days (smoother long-term average). Optuna identifies this automatically per combination rather than using a one-size-fits-all window.

## Stopping and Restarting

```bash
# Stop (keeps database data intact)
docker compose down

# Restart (no rebuild needed, data persists)
docker compose up

# After restart: click Reset in browser, then Next Day
# (API loses in-memory simulation state on restart — DB data is preserved)
```

## Future Roadmap

- **CI/CD:** GitHub Actions pipeline for automated testing and image builds
- **Tests:** pytest suite for ETL, API endpoints, and simulation logic
- **Monitoring:** Prometheus + Grafana for Kafka lag, event throughput, API latency
- **Frontend:** Chart.js visualizations for revenue trends, top products, weather correlation
- **Multi-step forecast:** Rolling forward projection instead of static mean

## Technical Stack Summary

| Component | Technology |
|-----------|-----------|
| Language | Python 3.11 |
| API Framework | FastAPI + Uvicorn |
| Database | MySQL 8.0 |
| Message Broker | Confluent Kafka 7.6.0 (KRaft) |
| Optimization | Optuna 3.6.1 (TPE Bayesian search) |
| Containers | Docker + Docker Compose |
| Data Processing | pandas, numpy |
| Real-time Stream | Server-Sent Events (SSE) |
| DB Driver | mysql-connector-python (no ORM) |

## Author

Matt Raymond Ayento  
Nagoya University  
G30, 3rd year Automotive Engineering (Electrical, Electronics, Information Engineering)