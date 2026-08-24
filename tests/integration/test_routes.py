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


def make_client() -> TestClient:
    settings = Settings(spot_base_url=SPOT_BASE, futures_base_url=FUTURES_BASE)
    app = create_app(settings)
    return TestClient(app)


def binance_row(open_time: int, interval_ms: int = 60_000) -> list:
    close_time = open_time + interval_ms - 1
    return [open_time, "1", "2", "0.5", "1.5", "10", close_time, "15", 3, "1", "1", "0"]


class TestLifespan:
    def test_upstream_http_clients_are_closed_on_shutdown(self):
        settings = Settings(spot_base_url=SPOT_BASE, futures_base_url=FUTURES_BASE)
        app = create_app(settings)
        clients = list(app.state.service.clients.values())

        with TestClient(app):
            assert all(not c._http.is_closed for c in clients)

        assert all(c._http.is_closed for c in clients)


class TestHealthAndStats:
    def test_healthz_returns_ok(self):
        client = make_client()
        response = client.get("/healthz")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    def test_stats_exposes_cache_and_breaker_state(self):
        client = make_client()
        response = client.get("/stats")
        assert response.status_code == 200
        body = response.json()
        assert "coalescing" in body
        assert "cache" in body
        assert "spot" in body["markets"]
        assert "usdm_futures" in body["markets"]
        assert body["markets"]["spot"]["banned"] is False
        assert body["markets"]["spot"]["upstream_calls_made"] == 0

    def test_stats_upstream_calls_made_reflects_actual_binance_hits(self, respx_mock):
        respx_mock.get(f"{SPOT_BASE}/api/v3/klines").mock(
            return_value=httpx.Response(200, json=[binance_row(0)])
        )
        client = make_client()
        params = {"symbol": "BTCUSDT", "interval": "1m", "limit": 1}

        client.get("/api/v3/klines", params=params)
        client.get("/api/v3/klines", params=params)  # within TTL -> cache hit

        body = client.get("/stats").json()
        assert body["markets"]["spot"]["upstream_calls_made"] == 1
        assert body["cache"]["hits"] == 1


class TestKlinesResponseFidelity:
    def test_spot_klines_returns_binance_shaped_array_of_arrays(self, respx_mock):
        respx_mock.get(f"{SPOT_BASE}/api/v3/klines").mock(
            return_value=httpx.Response(200, json=[binance_row(0)])
        )
        client = make_client()

        response = client.get(
            "/api/v3/klines",
            params={"symbol": "BTCUSDT", "interval": "1m", "limit": 1},
        )

        assert response.status_code == 200
        assert response.json() == [binance_row(0)]

    def test_arbitrary_binance_params_are_forwarded_verbatim(self, respx_mock):
        route = respx_mock.get(f"{SPOT_BASE}/api/v3/klines").mock(
            return_value=httpx.Response(200, json=[binance_row(0)])
        )
        client = make_client()

        client.get(
            "/api/v3/klines",
            params={
                "symbol": "BTCUSDT",
                "interval": "1d",
                "startTime": 123,
                "endTime": 456,
                "timeZone": "-08:00",
                "limit": 3,
            },
        )

        sent = route.calls[0].request.url.params
        assert sent["startTime"] == "123"
        assert sent["endTime"] == "456"
        assert sent["timeZone"] == "-08:00"
        assert sent["limit"] == "3"

    def test_futures_klines_hits_the_futures_base_url(self, respx_mock):
        route = respx_mock.get(f"{FUTURES_BASE}/fapi/v1/klines").mock(
            return_value=httpx.Response(200, json=[binance_row(0)])
        )
        client = make_client()

        response = client.get(
            "/fapi/v1/klines",
            params={"symbol": "BTCUSDT", "interval": "1m", "limit": 1},
        )

        assert response.status_code == 200
        assert route.call_count == 1


class TestErrorPassthrough:
    def test_binance_client_error_is_passed_through_verbatim(self, respx_mock):
        error_body = {"code": -1121, "msg": "Invalid symbol."}
        respx_mock.get(f"{SPOT_BASE}/api/v3/klines").mock(
            return_value=httpx.Response(400, json=error_body)
        )
        client = make_client()

        response = client.get(
            "/api/v3/klines",
            params={"symbol": "NOTREAL", "interval": "1m", "limit": 1},
        )

        assert response.status_code == 400
        assert response.json() == error_body

    def test_binance_error_is_not_cached_asks_again_next_time(self, respx_mock):
        route = respx_mock.get(f"{SPOT_BASE}/api/v3/klines").mock(
            side_effect=[
                httpx.Response(400, json={"code": -1121, "msg": "Invalid symbol."}),
                httpx.Response(200, json=[binance_row(0)]),
            ]
        )
        client = make_client()
        params = {"symbol": "BADSYM", "interval": "1m", "limit": 1}

        first = client.get("/api/v3/klines", params=params)
        second = client.get("/api/v3/klines", params=params)

        assert first.status_code == 400
        assert second.status_code == 200
        assert route.call_count == 2


class TestUpstreamUnavailableHandling:
    def test_transport_failure_results_in_503_not_a_crash(self, respx_mock):
        respx_mock.get(f"{SPOT_BASE}/api/v3/klines").mock(side_effect=httpx.ConnectError("boom"))
        client = make_client()

        response = client.get(
            "/api/v3/klines",
            params={"symbol": "BTCUSDT", "interval": "1m", "limit": 1},
        )

        assert response.status_code == 503

    def test_non_json_body_results_in_503_not_a_crash(self, respx_mock):
        respx_mock.get(f"{SPOT_BASE}/api/v3/klines").mock(
            return_value=httpx.Response(200, content=b"not json")
        )
        client = make_client()

        response = client.get(
            "/api/v3/klines",
            params={"symbol": "BTCUSDT", "interval": "1m", "limit": 1},
        )

        assert response.status_code == 503

    def test_malformed_limit_reaches_binance_instead_of_crashing(self, respx_mock):
        route = respx_mock.get(f"{SPOT_BASE}/api/v3/klines").mock(
            return_value=httpx.Response(
                400, json={"code": -1130, "msg": "Data sent for parameter 'limit' is not valid."}
            )
        )
        client = make_client()

        response = client.get(
            "/api/v3/klines",
            params={"symbol": "BTCUSDT", "interval": "1m", "limit": "not-a-number"},
        )

        assert route.call_count == 1
        assert response.status_code == 400


class TestBanHandling:
    def test_418_from_binance_results_in_503_with_retry_after_on_next_uncached_request(
        self, respx_mock
    ):
        route = respx_mock.get(f"{SPOT_BASE}/api/v3/klines")
        route.side_effect = [
            httpx.Response(
                418, json={"code": -1003, "msg": "banned"}, headers={"Retry-After": "60"}
            ),
        ]
        client = make_client()

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
