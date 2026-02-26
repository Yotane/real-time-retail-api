"""
Docker compose up must be running.

Usage:
    pip install pytest httpx
    pytest tests/test_api.py -v
"""

import pytest
import httpx

BASE_URL = "http://localhost:8000"


@pytest.fixture(scope="session")
def client():
    with httpx.Client(base_url=BASE_URL, timeout=60.0) as c:
        yield c


@pytest.fixture(scope="session")
def valid_ids(client):
    """
    Fetch a real store_id + product_id that has 20+ days in sales_facts.
    Uses day=731 to force full history regardless of simulation state.
    """
    r = client.get("/sales/valid-combo")
    if r.status_code != 200:
        pytest.skip("No valid store/product combo found")
    data = r.json()
    return data["store_id"], data["product_id"]


class TestHealth:
    def test_health_returns_200(self, client):
        r = client.get("/health")
        assert r.status_code == 200

    def test_health_response_shape(self, client):
        data = client.get("/health").json()
        assert data["status"] == "ok"
        assert data["service"] == "api"


class TestSimulationStatus:
    def test_status_returns_200(self, client):
        assert client.get("/simulation/status").status_code == 200

    def test_status_has_required_fields(self, client):
        data = client.get("/simulation/status").json()
        for field in ["current_day", "day_index", "total_days", "day_total",
                      "day_received", "is_running", "is_complete", "progress_pct"]:
            assert field in data

    def test_status_field_types(self, client):
        data = client.get("/simulation/status").json()
        assert isinstance(data["day_index"],    int)
        assert isinstance(data["total_days"],   int)
        assert isinstance(data["is_running"],   bool)
        assert isinstance(data["is_complete"],  bool)
        assert isinstance(data["progress_pct"], (int, float))

    def test_progress_pct_in_valid_range(self, client):
        data = client.get("/simulation/status").json()
        assert 0.0 <= data["progress_pct"] <= 100.0


class TestSimulationReset:
    def test_reset_returns_200(self, client):
        assert client.post("/simulation/reset").status_code == 200

    def test_reset_response(self, client):
        assert client.post("/simulation/reset").json()["status"] == "reset"

    def test_reset_clears_state(self, client):
        client.post("/simulation/reset")
        data = client.get("/simulation/status").json()
        assert data["current_day"] is None
        assert data["day_index"] == -1
        assert data["is_running"] is False
        assert data["is_complete"] is False


class TestSimulationNextDay:
    def test_next_day_400_when_not_initialized(self, client):
        client.post("/simulation/reset")
        status = client.get("/simulation/status").json()
        if status["total_days"] == 0:
            assert client.post("/simulation/next-day").status_code == 400

    def test_next_day_advances_when_initialized(self, client):
        if client.get("/simulation/status").json()["total_days"] == 0:
            pytest.skip("Producer not running")
        client.post("/simulation/reset")
        r = client.post("/simulation/next-day")
        assert r.status_code == 200
        assert r.json()["current_day"] is not None

    def test_next_day_increments_index(self, client):
        if client.get("/simulation/status").json()["total_days"] == 0:
            pytest.skip("Producer not running")
        client.post("/simulation/reset")
        client.post("/simulation/next-day")
        assert client.get("/simulation/status").json()["day_index"] == 0

    def test_next_day_returns_valid_date(self, client):
        if client.get("/simulation/status").json()["total_days"] == 0:
            pytest.skip("Producer not running")
        client.post("/simulation/reset")
        day = client.post("/simulation/next-day").json()["current_day"]
        parts = day.split("-")
        assert len(parts) == 3 and len(parts[0]) == 4


class TestSalesRecent:
    def test_returns_200(self, client):
        assert client.get("/sales/recent").status_code == 200

    def test_has_events_list(self, client):
        data = client.get("/sales/recent").json()
        assert "events" in data
        assert isinstance(data["events"], list)

    def test_limit_param(self, client):
        events = client.get("/sales/recent?limit=5").json()["events"]
        assert len(events) <= 5

    def test_default_limit_is_20(self, client):
        assert len(client.get("/sales/recent").json()["events"]) <= 20

    def test_event_shape(self, client):
        events = client.get("/sales/recent?limit=1").json()["events"]
        if not events:
            pytest.skip("No events yet — run simulation first")
        for field in ["store_id", "product_id", "date", "units_sold", "price"]:
            assert field in events[0]


class TestOptimizeHistory:
    def test_returns_200(self, client):
        assert client.get("/optimize/history").status_code == 200

    def test_response_shape(self, client):
        data = client.get("/optimize/history").json()
        if "message" in data:
            pytest.skip("No optimization runs yet")
        assert "total_trials" in data
        assert "best_trial" in data
        assert "trials" in data

    def test_filter_by_store_and_product(self, client, valid_ids):
        store_id, product_id = valid_ids
        assert client.get(
            f"/optimize/history?store_id={store_id}&product_id={product_id}"
        ).status_code == 200

    def test_best_trial_shape(self, client):
        data = client.get("/optimize/history").json()
        if "message" in data:
            pytest.skip("No optimization runs yet")
        for field in ["rmse", "window", "min_periods", "trend_window"]:
            assert field in data["best_trial"]

    def test_rmse_is_positive(self, client):
        data = client.get("/optimize/history").json()
        if "message" in data or not data["trials"]:
            pytest.skip("No optimization runs yet")
        for trial in data["trials"]:
            assert float(trial["rmse"]) > 0


class TestOptimizeDemand:
    """
    All tests pass day=731 to force full history, bypassing simulation
    cutoff which would limit data to only days seen so far.
    """

    def test_returns_200(self, client, valid_ids):
        store_id, product_id = valid_ids
        r = client.post("/optimize/demand",
            params={"store_id": store_id, "product_id": product_id,
                    "n_trials": 5, "day": 731})
        assert r.status_code == 200

    def test_forecast_appears_before_optimization(self, client, valid_ids):
        store_id, product_id = valid_ids
        r = client.post("/optimize/demand",
            params={"store_id": store_id, "product_id": product_id,
                    "n_trials": 5, "day": 731})
        keys = list(r.json().keys())
        assert keys.index("forecast") < keys.index("optimization")

    def test_forecast_shape(self, client, valid_ids):
        store_id, product_id = valid_ids
        r = client.post("/optimize/demand",
            params={"store_id": store_id, "product_id": product_id,
                    "n_trials": 5, "day": 731})
        f = r.json()["forecast"]
        for field in ["forecast_next_day", "trend", "7_day_forward", "last_5_actuals"]:
            assert field in f

    def test_7_day_forward_has_7_entries(self, client, valid_ids):
        store_id, product_id = valid_ids
        r = client.post("/optimize/demand",
            params={"store_id": store_id, "product_id": product_id,
                    "n_trials": 5, "day": 731})
        assert len(r.json()["forecast"]["7_day_forward"]) == 7

    def test_optimization_shape(self, client, valid_ids):
        store_id, product_id = valid_ids
        r = client.post("/optimize/demand",
            params={"store_id": store_id, "product_id": product_id,
                    "n_trials": 5, "day": 731})
        opt = r.json()["optimization"]
        for field in ["best_rmse", "best_params", "top_5_trials", "all_trials"]:
            assert field in opt

    def test_all_trials_count_matches_n_trials(self, client, valid_ids):
        store_id, product_id = valid_ids
        r = client.post("/optimize/demand",
            params={"store_id": store_id, "product_id": product_id,
                    "n_trials": 5, "day": 731})
        assert len(r.json()["optimization"]["all_trials"]) == 5

    def test_trend_is_valid(self, client, valid_ids):
        store_id, product_id = valid_ids
        r = client.post("/optimize/demand",
            params={"store_id": store_id, "product_id": product_id,
                    "n_trials": 5, "day": 731})
        assert r.json()["forecast"]["trend"] in ("up", "down", "stable")

    def test_n_trials_too_low(self, client):
        r = client.post("/optimize/demand",
            params={"store_id": "S001", "product_id": "P0001", "n_trials": 2})
        assert r.status_code == 400

    def test_n_trials_too_high(self, client):
        r = client.post("/optimize/demand",
            params={"store_id": "S001", "product_id": "P0001", "n_trials": 51})
        assert r.status_code == 400

    def test_invalid_product_returns_404(self, client):
        r = client.post("/optimize/demand",
            params={"store_id": "S001", "product_id": "FAKE",
                    "n_trials": 5, "day": 731})
        assert r.status_code == 404