import os
import logging
import mysql.connector
import optuna
import numpy as np
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)
optuna.logging.set_verbosity(optuna.logging.WARNING)


def get_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", 3306)),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE")
    )


def fetch_sales_history(store_id: str, product_id: str, cutoff: str = None) -> list:
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
    return rows


def compute_rmse(actual: list, predicted: list) -> float:
    a = np.array(actual,    dtype=float)
    p = np.array(predicted, dtype=float)
    return float(np.sqrt(np.mean((a - p) ** 2)))


def moving_average_forecast(series: list, window: int, min_periods: int) -> list:
    """Sliding window moving average over entire series."""
    predictions = []
    for i in range(len(series)):
        start = max(0, i - window)
        chunk = series[start:i]
        if len(chunk) < min_periods:
            predictions.append(float(np.mean(series[:max(1, i)])) if i > 0 else float(series[0]))
        else:
            predictions.append(float(np.mean(chunk)))
    return predictions


def save_trial(conn, study_name, trial_number, store_id, product_id,
               window, min_periods, trend_window, rmse):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO optuna_trials
            (study_name, trial_number, store_id, product_id,
             _window, min_periods, trend_window, rmse, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'complete')
    """, (study_name, trial_number, store_id, product_id,
          window, min_periods, trend_window, round(rmse, 4)))
    conn.commit()
    cursor.close()


def run_optimization(store_id: str, product_id: str, n_trials: int = 20, cutoff: str = None) -> dict:
    """
    1. Run Optuna to find best forecasting parameters.
    2. Apply those parameters to make an actual demand forecast.
    3. Return trial summary + forecast together.
    """
    study_name = f"{store_id}_{product_id}"
    conn       = get_connection()

    rows = fetch_sales_history(store_id, product_id, cutoff)
    if len(rows) < 20:
        conn.close()
        return {"error": f"Need 20+ days of data, got {len(rows)}."}

    series = [float(r["units_sold"]) for r in rows]
    dates  = [str(r["date"]) for r in rows]

    split       = int(len(series) * 0.8)
    test_actual = series[split:]

    trial_log = []

    def objective(trial):
        window       = trial.suggest_int("window",       3, 30)
        min_periods  = trial.suggest_int("min_periods",  1,  7)
        trend_window = trial.suggest_int("trend_window", 5, 21)

        preds      = moving_average_forecast(series, window, min_periods)
        test_preds = preds[split:]
        rmse       = compute_rmse(test_actual, test_preds)

        save_trial(conn, study_name, trial.number, store_id, product_id,
                   window, min_periods, trend_window, rmse)

        trial_log.append({
            "trial":        trial.number + 1,
            "window":       window,
            "min_periods":  min_periods,
            "trend_window": trend_window,
            "rmse":         round(rmse, 4)
        })
        return rmse

    study = optuna.create_study(
        direction="minimize",
        study_name=study_name,
        sampler=optuna.samplers.TPESampler(seed=42)
    )
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)
    conn.close()

    best   = study.best_params
    best_w = best["window"]
    best_m = best["min_periods"]

    all_preds    = moving_average_forecast(series, best_w, best_m)
    forecast_val = round(float(np.mean(series[-best_w:])), 2)
    forward_7    = [{"day": i + 1, "forecast_units": forecast_val} for i in range(7)]

    if len(series) >= 14:
        recent_avg   = float(np.mean(series[-7:]))
        previous_avg = float(np.mean(series[-14:-7]))
        trend_pct    = round(((recent_avg - previous_avg) / (previous_avg + 1e-9)) * 100, 1)
        trend        = "up" if trend_pct > 2 else "down" if trend_pct < -2 else "stable"
    else:
        trend_pct = 0.0
        trend     = "stable"

    sorted_trials   = sorted(trial_log, key=lambda x: x["rmse"])
    first_rmse      = trial_log[0]["rmse"]
    best_rmse       = round(study.best_value, 4)
    improvement_pct = round((1 - best_rmse / (first_rmse + 1e-9)) * 100, 1)

    return {
        "store_id":           store_id,
        "product_id":         product_id,
        "analysis_up_to_day": cutoff or "all",

        "forecast": {
            "method":            "moving_average_optimized",
            "window_used":       best_w,
            "min_periods_used":  best_m,
            "history_days_used": len(series),
            "forecast_next_day": forecast_val,
            "trend":             trend,
            "trend_pct":         trend_pct,
            "7_day_forward":     forward_7,
            "last_5_actuals": [
                {"date": dates[i], "units_sold": series[i]}
                for i in range(-5, 0)
            ]
        },

        "optimization": {
            "n_trials":         n_trials,
            "study_name":       study_name,
            "first_trial_rmse": first_rmse,
            "best_rmse":        best_rmse,
            "improvement_pct":  improvement_pct,
            "best_params": {
                "window":       best["window"],
                "min_periods":  best["min_periods"],
                "trend_window": best["trend_window"]
            },
            "top_5_trials":   sorted_trials[:5],
            "worst_5_trials": sorted_trials[-5:],
            "all_trials":     sorted(trial_log, key=lambda x: x["trial"])
        },
    }