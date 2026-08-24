"""End-to-end HTTP tests: a real FastAPI app, a respx-mocked Binance, and
assertions on exact response shape/status/headers as a desk client would see
them. This is where the "same signature as Binance" and "503 + Retry-After
on ban" requirements are verified.
"""

import httpx
from fastapi.testclient import TestClient

from binance_proxy.app import create_app
from binance_proxy.config import Settings

SPOT_BASE = "https://api.binance.com"
FUTURES_BASE = "https://fapi.binance.com"


def make_client(tmp_path) -> TestClient:
    settings = Settings(
        spot_base_url=SPOT_BASE,
        futures_base_url=FUTURES_BASE,
        data_dir=tmp_path,
    )
    app = create_app(settings)
    return TestClient(app)


def binance_row(open_time: int, interval_ms: int = 60_000) -> list:
    close_time = open_time + interval_ms - 1
    return [open_time, "1", "2", "0.5", "1.5", "10", close_time, "15", 3, "1", "1", "0"]


class TestHealthAndStats:
    def test_healthz_returns_ok(self, tmp_path):
        client = make_client(tmp_path)
        response = client.get("/healthz")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    def test_stats_exposes_cache_and_breaker_state(self, tmp_path):
        client = make_client(tmp_path)
        response = client.get("/stats")
        assert response.status_code == 200
        body = response.json()
        assert "coalescing" in body
        assert "spot" in body["markets"]
        assert "usdm_futures" in body["markets"]
        assert body["markets"]["spot"]["banned"] is False
        assert body["markets"]["spot"]["upstream_calls_made"] == 0


    def test_stats_upstream_calls_made_reflects_actual_binance_hits(self, tmp_path, respx_mock):
        respx_mock.get(f"{SPOT_BASE}/api/v3/klines").mock(
            return_value=httpx.Response(200, json=[binance_row(0)])
        )
        client = make_client(tmp_path)
        # An explicit startTime routes through the cache/gap-fill path (a
        # startTime-less request is always a live "tail" passthrough).
        params = {"symbol": "BTCUSDT", "interval": "1m", "limit": 1, "startTime": 0}

        client.get("/api/v3/klines", params=params)
        client.get("/api/v3/klines", params=params)  # fully covered by the first call now

        body = client.get("/stats").json()
        assert body["markets"]["spot"]["upstream_calls_made"] == 1


class TestKlinesResponseFidelity:
    def test_spot_klines_returns_binance_shaped_array_of_arrays(self, tmp_path, respx_mock):
        respx_mock.get(f"{SPOT_BASE}/api/v3/klines").mock(
            return_value=httpx.Response(200, json=[binance_row(0)])
        )
        client = make_client(tmp_path)

        response = client.get(
            "/api/v3/klines",
            params={"symbol": "BTCUSDT", "interval": "1m", "limit": 1},
        )

        assert response.status_code == 200
        assert response.json() == [binance_row(0)]

    def test_futures_klines_hits_the_futures_base_url(self, tmp_path, respx_mock):
        route = respx_mock.get(f"{FUTURES_BASE}/fapi/v1/klines").mock(
            return_value=httpx.Response(200, json=[binance_row(0)])
        )
        client = make_client(tmp_path)

        response = client.get(
            "/fapi/v1/klines",
            params={"symbol": "BTCUSDT", "interval": "1m", "limit": 1},
        )

        assert response.status_code == 200
        assert route.call_count == 1


class TestErrorPassthrough:
    def test_binance_client_error_is_passed_through_verbatim(self, tmp_path, respx_mock):
        error_body = {"code": -1121, "msg": "Invalid symbol."}
        respx_mock.get(f"{SPOT_BASE}/api/v3/klines").mock(
            return_value=httpx.Response(400, json=error_body)
        )
        client = make_client(tmp_path)

        response = client.get(
            "/api/v3/klines",
            params={"symbol": "NOTREAL", "interval": "1m", "limit": 1},
        )

        assert response.status_code == 400
        assert response.json() == error_body

    def test_invalid_limit_is_rejected_before_calling_binance(self, tmp_path, respx_mock):
        client = make_client(tmp_path)

        response = client.get(
            "/api/v3/klines",
            params={"symbol": "BTCUSDT", "interval": "1m", "limit": 5000},
        )

        assert response.status_code == 400
        assert respx_mock.calls.call_count == 0


class TestBanHandling:
    def test_418_from_binance_results_in_503_with_retry_after_on_next_uncached_request(
        self, tmp_path, respx_mock
    ):
        route = respx_mock.get(f"{SPOT_BASE}/api/v3/klines")
        route.side_effect = [
            httpx.Response(
                418, json={"code": -1003, "msg": "banned"}, headers={"Retry-After": "60"}
            ),
        ]
        client = make_client(tmp_path)

        first = client.get(
            "/api/v3/klines",
            params={"symbol": "BTCUSDT", "interval": "1m", "limit": 1},
        )
        assert first.status_code == 503

        # Breaker is now open; a second, still-uncached request must not hit
        # Binance again and must also come back as 503 + Retry-After.
        second = client.get(
            "/api/v3/klines",
            params={"symbol": "ETHUSDT", "interval": "1m", "limit": 1},
        )
        assert second.status_code == 503
        assert "Retry-After" in second.headers
        assert route.call_count == 1
