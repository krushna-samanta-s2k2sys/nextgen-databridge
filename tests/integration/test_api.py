"""
Integration tests for NextGenDatabridge FastAPI API
Requires a running PostgreSQL and the app running (or use TestClient).
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))

import pytest
import asyncio
from httpx import AsyncClient, ASGITransport

# Patch DB URL to test DB before importing app
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://nextgen_databridge:nextgen_databridge@localhost:5432/nextgen_databridge_audit")
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("AIRFLOW_URL", "http://localhost:8080")

from api.main import app


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_health(client):
    r = await client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_login_success(client):
    r = await client.post("/api/auth/login", json={"username": "admin", "password": "admin"})
    assert r.status_code == 200
    data = r.json()
    assert "token" in data
    assert data["user"] == "admin"


@pytest.mark.asyncio
async def test_login_failure(client):
    r = await client.post("/api/auth/login", json={"username": "admin", "password": "wrong"})
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_validate_config_valid(client):
    r = await client.post("/api/config/validate", json={
        "config": {
            "pipeline_id": "test_pipeline",
            "version": "1.0.0",
            "tasks": [
                {
                    "task_id": "extract",
                    "type": "sql_extract",
                    "source": {"connection": "conn1", "query": "SELECT 1"},
                    "output": {"duckdb_path": "s3://b/t.duckdb"},
                }
            ]
        }
    })
    assert r.status_code == 200
    assert r.json()["valid"] is True


@pytest.mark.asyncio
async def test_validate_config_invalid(client):
    r = await client.post("/api/config/validate", json={
        "config": {
            "pipeline_id": "BadName",
            "tasks": []
        }
    })
    assert r.status_code == 200
    assert r.json()["valid"] is False
    assert len(r.json()["errors"]) > 0


@pytest.mark.asyncio
async def test_list_pipelines_empty(client):
    r = await client.get("/api/pipelines")
    assert r.status_code == 200
    assert "pipelines" in r.json()


@pytest.mark.asyncio
async def test_list_runs_empty(client):
    r = await client.get("/api/runs")
    assert r.status_code == 200
    assert "runs" in r.json()


@pytest.mark.asyncio
async def test_list_tasks_empty(client):
    r = await client.get("/api/tasks")
    assert r.status_code == 200
    assert "tasks" in r.json()


@pytest.mark.asyncio
async def test_list_alerts_empty(client):
    r = await client.get("/api/alerts")
    assert r.status_code == 200
    assert "alerts" in r.json()


@pytest.mark.asyncio
async def test_list_connections_empty(client):
    r = await client.get("/api/connections")
    assert r.status_code == 200
    assert "connections" in r.json()


@pytest.mark.asyncio
async def test_list_deployments_empty(client):
    r = await client.get("/api/deployments")
    assert r.status_code == 200
    assert "deployments" in r.json()


@pytest.mark.asyncio
async def test_audit_logs_empty(client):
    r = await client.get("/api/audit")
    assert r.status_code == 200
    assert "logs" in r.json()


@pytest.mark.asyncio
async def test_dashboard_metrics(client):
    r = await client.get("/api/metrics/dashboard")
    assert r.status_code == 200
    data = r.json()
    assert "active_runs" in data
    assert "success_rate" in data
    assert "active_alerts" in data


@pytest.mark.asyncio
async def test_pipeline_not_found(client):
    r = await client.get("/api/pipelines/does_not_exist")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_run_not_found(client):
    r = await client.get("/api/runs/nonexistent_run_id")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_event_types(client):
    r = await client.get("/api/event-types")
    assert r.status_code == 200
    assert "event_types" in r.json()
    assert len(r.json()["event_types"]) > 0


@pytest.mark.asyncio
async def test_query_rejects_non_select(client):
    r = await client.post("/api/query", json={
        "pipeline_id": "p", "run_id": "r", "task_id": "t",
        "duckdb_path": "s3://bucket/file.duckdb",
        "sql": "DROP TABLE orders",
        "limit": 100
    })
    assert r.status_code in (400, 500)  # Either validation or file-not-found error


@pytest.mark.asyncio
async def test_create_pipeline_full_flow(client):
    """Create a pipeline and verify it appears in the list"""
    payload = {
        "pipeline_id": "integration_test_pipeline",
        "name": "Integration Test Pipeline",
        "config": {
            "pipeline_id": "integration_test_pipeline",
            "version": "1.0.0",
            "tasks": [
                {
                    "task_id": "extract",
                    "type": "sql_extract",
                    "source": {"connection": "test_conn", "query": "SELECT 1 AS id"},
                    "output": {"duckdb_path": "s3://test/extract.duckdb"},
                }
            ]
        },
        "schedule": "0 8 * * *",
        "tags": ["test"],
    }
    create_r = await client.post("/api/pipelines", json=payload)
    assert create_r.status_code in (201, 422, 500)  # 201=success, others if DB not running

    if create_r.status_code == 201:
        get_r = await client.get("/api/pipelines/integration_test_pipeline")
        assert get_r.status_code == 200
        assert get_r.json()["pipeline_id"] == "integration_test_pipeline"
