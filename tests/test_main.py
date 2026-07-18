import pytest
from fastapi.testclient import TestClient
from prometheus_client import REGISTRY
from app.main import app, state

client = TestClient(app)

@pytest.fixture(autouse=True)
def setup_test_state():
    # Capture initial value of metric counter before individual test mutations
    before_value = REGISTRY.get_sample_value("ingest_messages_total") or 0.0
    yield before_value

def test_health_check_fail_if_redis_none():
    state.redis_client = None
    response = client.get("/health")
    assert response.status_code == 503

def test_ingest_validation_failure():
    # Post incomplete payload lacking structural content properties
    response = client.post("/ingest", json={"doc_id": "test-doc-001"})
    assert response.status_code == 422  # Unprocessable Entity via Pydantic

def test_metrics_endpoint_render():
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "ingest_messages_total" in response.text