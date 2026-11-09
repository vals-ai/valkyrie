import httpx
import pytest
from fastapi.testclient import TestClient


@pytest.mark.description("Benchmark service endpoint responses and authentication.")
class TestBenchmarkServices:
    @pytest.mark.description(
        (
            "Service health aggregation must keep healthy and failed entries distinct.\n"
            "\n"
            "Test cases:\n"
            "- A reachable service reports healthy while a failing service retains its HTTP status."
        )
    )
    def test_benchmark_services_returns_pings(self, client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:

        async def handle_request(request: httpx.Request) -> httpx.Response:
            status_code = 200 if request.url.host == "up" else 503
            return httpx.Response(status_code, request=request)

        transport = httpx.MockTransport(handle_request)
        original_client = httpx.AsyncClient

        def build_client(*, timeout: float) -> httpx.AsyncClient:
            return original_client(transport=transport, timeout=timeout)

        monkeypatch.setattr(httpx, "AsyncClient", build_client)

        response = client.post(
            "/benchmark-services",
            headers={"X-Api-Key": "fake"},
            json={
                "services": [
                    {"name": "swebench", "url": "http://up:8001"},
                    {"name": "fab", "url": "http://down:8002"},
                ]
            },
        )
        assert response.status_code == 200, response.text
        response_body = response.json()
        services_by_name = {service["name"]: service for service in response_body["services"]}
        assert services_by_name["swebench"]["healthy"] is True
        assert services_by_name["fab"]["healthy"] is False
        assert services_by_name["fab"]["error"] == "HTTP 503"

    @pytest.mark.description(
        (
            "Benchmark-service health data must require authentication.\n"
            "\n"
            "Test cases:\n"
            "- A request without a bearer session receives 401."
        )
    )
    def test_benchmark_services_unauth_401(self, client: TestClient) -> None:
        response = client.post("/benchmark-services", json={"services": []})

        assert response.status_code == 401
