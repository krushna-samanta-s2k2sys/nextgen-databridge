"""
NextGenDatabridge Platform API
Full FastAPI backend with REST API, WebSocket live updates, and JWT auth.
"""
from __future__ import annotations

import json
import logging
import os
import secrets
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import httpx
from fastapi import (
    FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect,
    BackgroundTasks, Query, Path, Body, status
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator
from sqlalchemy import select, desc, func, and_, or_, update
from sqlalchemy.ext.asyncio import AsyncSession

import jwt

from models.database import get_db, create_tables
from models.models import (
    Pipeline, PipelineConfig, PipelineRun, TaskRun, AuditLog, DataConnection,
    Alert, AlertRule, Deployment, DuckDBFile, EKSJob, PipelineMetrics,
    PipelineStatus, RunStatus, TaskStatus, DeploymentStatus, DeploymentEnvironment,
    AuditEventType, AlertSeverity, ConnectionType,
)
from services.audit_service import audit
from services.config_validator import PipelineConfigValidator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("nextgen_databridge.api")


def _to_enum(enum_cls, value: Optional[str]):
    """Convert a URL query param string to an enum instance (by name, case-insensitive)."""
    if not value:
        return None
    try:
        return enum_cls[value.upper()]
    except KeyError:
        return None

SECRET_KEY   = os.getenv("SECRET_KEY", "nextgen-databridge-secret-change-in-prod")
ALGORITHM    = "HS256"
TOKEN_EXPIRY = timedelta(hours=24)

AIRFLOW_URL  = os.getenv("AIRFLOW_URL", "http://airflow-webserver:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASS = os.getenv("AIRFLOW_PASS", "admin")


# ─────────────────────────────────────────────────────────────────────────────
# Lifespan
# ─────────────────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("NextGenDatabridge API starting up...")
    try:
        await create_tables()
        logger.info("Database tables ensured")
    except Exception as exc:
        # Non-fatal: pod must start so the readiness probe can pass.
        # DB-dependent endpoints will fail with 503 until connectivity is restored.
        logger.error(f"Database startup error (will retry on first request): {exc}")
    yield
    logger.info("NextGenDatabridge API shutting down")


app = FastAPI(
    title="NextGenDatabridge Platform API",
    description="Modern ELT Pipeline Platform — REST API",
    version="2.4.0",
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
)

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────────────────────────────────────────
# Auth
# ─────────────────────────────────────────────────────────────────────────────
bearer_scheme = HTTPBearer(auto_error=False)

def create_token(user: str, role: str = "user") -> str:
    payload = {
        "sub": user,
        "role": role,
        "exp": datetime.now(timezone.utc) + TOKEN_EXPIRY,
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme)) -> dict:
    if not credentials:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

# Optional auth (allows unauthenticated for dev)
def maybe_token(credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme)) -> Optional[dict]:
    if not credentials:
        return {"sub": "dev", "role": "admin"}
    try:
        return jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
    except Exception:
        return {"sub": "anonymous", "role": "viewer"}


# ─────────────────────────────────────────────────────────────────────────────
# WebSocket Connection Manager
# ─────────────────────────────────────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, data: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

ws_manager = ConnectionManager()


# ─────────────────────────────────────────────────────────────────────────────
# Pydantic Schemas
# ─────────────────────────────────────────────────────────────────────────────
class LoginRequest(BaseModel):
    username: str
    password: str

class LoginResponse(BaseModel):
    token: str
    user: str
    role: str

class PipelineCreateRequest(BaseModel):
    pipeline_id: str
    name: str
    description: Optional[str] = None
    config: dict
    schedule: Optional[str] = None
    tags: List[str] = []
    owner: Optional[str] = None
    sla_minutes: Optional[int] = None

class PipelineUpdateRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    config: Optional[dict] = None
    schedule: Optional[str] = None
    tags: Optional[List[str]] = None
    sla_minutes: Optional[int] = None

class ValidateConfigRequest(BaseModel):
    config: dict

class TriggerRunRequest(BaseModel):
    trigger_type: str = "manual"
    conf: Optional[dict] = None

class RerunTaskRequest(BaseModel):
    task_id: str
    mode: str = "downstream"  # downstream, single, full
    reason: str = ""

class ConnectionCreateRequest(BaseModel):
    connection_id: str
    name: str
    connection_type: str
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    schema_name: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    extra_config: Optional[dict] = None

class DeploymentCreateRequest(BaseModel):
    pipeline_id: str
    version: str
    deployment_type: str = "config"
    environment: str = "staging"
    change_description: Optional[str] = None
    approver_email: Optional[str] = None
    container_image: Optional[str] = None

class AlertRuleCreateRequest(BaseModel):
    pipeline_id: Optional[str] = None
    rule_name: str
    condition: str
    threshold: Optional[dict] = None
    severity: str = "error"
    channels: List[str] = []

class QueryRequest(BaseModel):
    pipeline_id: str
    run_id: str
    task_id: str
    duckdb_path: str
    sql: str
    limit: int = Field(default=1000, le=10000)


# ─────────────────────────────────────────────────────────────────────────────
# Airflow client helper
# ─────────────────────────────────────────────────────────────────────────────
async def airflow_request(method: str, path: str, **kwargs) -> dict:
    async with httpx.AsyncClient(timeout=30) as client:
        url = f"{AIRFLOW_URL}/api/v1{path}"
        resp = await client.request(
            method, url,
            auth=(AIRFLOW_USER, AIRFLOW_PASS),
            **kwargs
        )
        if resp.status_code >= 400:
            logger.warning(f"Airflow API {method} {path} → {resp.status_code}: {resp.text[:200]}")
        return resp.json() if resp.text else {}


# ─────────────────────────────────────────────────────────────────────────────
# ── AUTH ──────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
@app.post("/api/auth/login", response_model=LoginResponse, tags=["Auth"])
async def login(req: LoginRequest):
    """Simple username/password login — replace with SSO/LDAP in production"""
    USERS = {
        "admin":   ("admin",   "admin"),
        "akumar":  ("akumar",  "user"),
        "jsmith":  ("jsmith",  "user"),
        "viewer":  ("viewer",  "viewer"),
    }
    user_entry = USERS.get(req.username)
    if not user_entry or req.password not in (req.username, "password", user_entry[1]):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    role  = user_entry[1]
    token = create_token(req.username, role)
    return {"token": token, "user": req.username, "role": role}

@app.get("/api/auth/me", tags=["Auth"])
async def me(current_user: dict = Depends(maybe_token)):
    return current_user


# ─────────────────────────────────────────────────────────────────────────────
# ── PIPELINES ─────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/pipelines", tags=["Pipelines"])
async def list_pipelines(
    status_filter: Optional[str] = Query(None, alias="status"),
    source_type: Optional[str] = None,
    search: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    q = select(Pipeline)
    if status_filter:
        ps = _to_enum(PipelineStatus, status_filter)
        if ps:
            q = q.where(Pipeline.status == ps)
    if source_type:
        q = q.where(Pipeline.source_type == source_type)
    if search:
        q = q.where(or_(
            Pipeline.pipeline_id.ilike(f"%{search}%"),
            Pipeline.name.ilike(f"%{search}%"),
        ))
    q = q.order_by(Pipeline.pipeline_id).offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(q)
    pipelines = result.scalars().all()

    # Enrich with latest run status
    enriched = []
    for p in pipelines:
        last_run = await db.execute(
            select(PipelineRun)
            .where(PipelineRun.pipeline_id == p.pipeline_id)
            .order_by(desc(PipelineRun.created_at))
            .limit(1)
        )
        run = last_run.scalar_one_or_none()
        enriched.append({
            "id": p.id, "pipeline_id": p.pipeline_id, "name": p.name,
            "description": p.description, "status": p.status.value,
            "current_version": p.current_version, "schedule": p.schedule,
            "source_type": p.source_type, "target_type": p.target_type,
            "tags": p.tags, "owner": p.owner, "sla_minutes": p.sla_minutes,
            "created_at": p.created_at.isoformat() if p.created_at else None,
            "last_run": {
                "run_id": run.run_id, "status": run.status.value,
                "start_time": run.start_time.isoformat() if run.start_time else None,
                "duration_seconds": run.duration_seconds,
            } if run else None,
        })
    return {"pipelines": enriched, "total": len(enriched)}


@app.post("/api/pipelines", status_code=201, tags=["Pipelines"])
async def create_pipeline(
    req: PipelineCreateRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    user = current_user.get("sub", "api")
    # Validate config
    validator = PipelineConfigValidator()
    vresult = validator.validate(req.config)

    # Create pipeline record
    pipeline = Pipeline(
        id=str(uuid.uuid4()),
        pipeline_id=req.pipeline_id,
        name=req.name,
        description=req.description,
        schedule=req.schedule,
        tags=req.tags,
        owner=req.owner,
        sla_minutes=req.sla_minutes,
        source_type=req.config.get("tasks", [{}])[0].get("source", {}).get("connection_type") if req.config.get("tasks") else None,
        created_by=user,
    )
    db.add(pipeline)

    # Create initial config version
    config_entry = PipelineConfig(
        id=str(uuid.uuid4()),
        pipeline_id=req.pipeline_id,
        version=req.config.get("version", "1.0.0"),
        config=req.config,
        is_active=True,
        is_valid=vresult.valid,
        validation_errors=vresult.to_dict() if not vresult.valid else None,
        created_by=user,
    )
    db.add(config_entry)
    await db.flush()

    background_tasks.add_task(
        audit.pipeline_created,
        req.pipeline_id, req.config, user
    )

    # Sync to Airflow
    background_tasks.add_task(_sync_dag_to_airflow, req.pipeline_id, req.config)

    return {
        "pipeline_id": req.pipeline_id,
        "version": config_entry.version,
        "validation": vresult.to_dict(),
    }


@app.get("/api/pipelines/{pipeline_id}", tags=["Pipelines"])
async def get_pipeline(
    pipeline_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    result = await db.execute(select(Pipeline).where(Pipeline.pipeline_id == pipeline_id))
    pipeline = result.scalar_one_or_none()
    if not pipeline:
        raise HTTPException(404, f"Pipeline '{pipeline_id}' not found")

    config_result = await db.execute(
        select(PipelineConfig)
        .where(PipelineConfig.pipeline_id == pipeline_id, PipelineConfig.is_active == True)
    )
    config = config_result.scalar_one_or_none()

    return {
        "pipeline_id": pipeline.pipeline_id,
        "name": pipeline.name,
        "description": pipeline.description,
        "status": pipeline.status.value,
        "current_version": pipeline.current_version,
        "schedule": pipeline.schedule,
        "config": config.config if config else None,
        "tags": pipeline.tags,
        "owner": pipeline.owner,
        "sla_minutes": pipeline.sla_minutes,
        "created_at": pipeline.created_at.isoformat() if pipeline.created_at else None,
    }


@app.put("/api/pipelines/{pipeline_id}", tags=["Pipelines"])
async def update_pipeline(
    pipeline_id: str,
    req: PipelineUpdateRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    user = current_user.get("sub", "api")
    result = await db.execute(select(Pipeline).where(Pipeline.pipeline_id == pipeline_id))
    pipeline = result.scalar_one_or_none()
    if not pipeline:
        raise HTTPException(404, "Pipeline not found")

    if req.name:        pipeline.name = req.name
    if req.description: pipeline.description = req.description
    if req.schedule:    pipeline.schedule = req.schedule
    if req.tags:        pipeline.tags = req.tags
    if req.sla_minutes: pipeline.sla_minutes = req.sla_minutes

    old_version = pipeline.current_version

    if req.config:
        # Validate new config
        validator = PipelineConfigValidator()
        vresult = validator.validate(req.config)

        new_version = req.config.get("version", pipeline.current_version or "1.0.0")
        pipeline.current_version = new_version

        # Deactivate old config
        await db.execute(
            update(PipelineConfig)
            .where(PipelineConfig.pipeline_id == pipeline_id)
            .values(is_active=False)
        )

        # Add new config
        config_entry = PipelineConfig(
            id=str(uuid.uuid4()),
            pipeline_id=pipeline_id,
            version=new_version,
            config=req.config,
            is_active=True,
            is_valid=vresult.valid,
            validation_errors=vresult.to_dict() if not vresult.valid else None,
            created_by=user,
        )
        db.add(config_entry)

        background_tasks.add_task(
            audit.config_updated, pipeline_id, old_version or "", new_version, user
        )
        background_tasks.add_task(_sync_dag_to_airflow, pipeline_id, req.config)

    return {"updated": True, "pipeline_id": pipeline_id}


@app.post("/api/pipelines/{pipeline_id}/pause", tags=["Pipelines"])
async def pause_pipeline(
    pipeline_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    await db.execute(
        update(Pipeline)
        .where(Pipeline.pipeline_id == pipeline_id)
        .values(status=PipelineStatus.PAUSED)
    )
    await airflow_request("PATCH", f"/dags/{pipeline_id}", json={"is_paused": True})
    return {"paused": True}


@app.post("/api/pipelines/{pipeline_id}/resume", tags=["Pipelines"])
async def resume_pipeline(
    pipeline_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    await db.execute(
        update(Pipeline)
        .where(Pipeline.pipeline_id == pipeline_id)
        .values(status=PipelineStatus.ACTIVE)
    )
    await airflow_request("PATCH", f"/dags/{pipeline_id}", json={"is_paused": False})
    return {"resumed": True}


# ─────────────────────────────────────────────────────────────────────────────
# ── CONFIG ────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
@app.post("/api/config/validate", tags=["Config"])
async def validate_config(req: ValidateConfigRequest):
    validator = PipelineConfigValidator()
    result = validator.validate(req.config)
    return result.to_dict()


@app.get("/api/pipelines/{pipeline_id}/configs", tags=["Config"])
async def get_config_versions(
    pipeline_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    result = await db.execute(
        select(PipelineConfig)
        .where(PipelineConfig.pipeline_id == pipeline_id)
        .order_by(desc(PipelineConfig.created_at))
    )
    configs = result.scalars().all()
    return {"versions": [
        {
            "version": c.version,
            "is_active": c.is_active,
            "is_valid": c.is_valid,
            "created_by": c.created_by,
            "created_at": c.created_at.isoformat() if c.created_at else None,
        }
        for c in configs
    ]}


@app.get("/api/pipelines/{pipeline_id}/configs/{version}", tags=["Config"])
async def get_config_version(
    pipeline_id: str,
    version: str,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    result = await db.execute(
        select(PipelineConfig)
        .where(PipelineConfig.pipeline_id == pipeline_id, PipelineConfig.version == version)
    )
    config = result.scalar_one_or_none()
    if not config:
        raise HTTPException(404, "Config version not found")
    return {"version": config.version, "config": config.config, "is_valid": config.is_valid,
            "validation_errors": config.validation_errors, "created_by": config.created_by}


# ─────────────────────────────────────────────────────────────────────────────
# ── RUNS ──────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
@app.post("/api/pipelines/{pipeline_id}/trigger", tags=["Runs"])
async def trigger_pipeline(
    pipeline_id: str,
    req: TriggerRunRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    user = current_user.get("sub", "api")

    # Trigger via Airflow API
    try:
        run_id = f"manual__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
        af_resp = await airflow_request(
            "POST", f"/dags/{pipeline_id}/dagRuns",
            json={"dag_run_id": run_id, "conf": req.conf or {}}
        )
        airflow_run_id = af_resp.get("dag_run_id", run_id)
    except Exception as e:
        logger.warning(f"Airflow trigger failed, creating DB record only: {e}")
        airflow_run_id = f"manual__{uuid.uuid4().hex[:8]}"

    # Create run record
    run = PipelineRun(
        id=str(uuid.uuid4()),
        run_id=airflow_run_id,
        pipeline_id=pipeline_id,
        status=RunStatus.QUEUED,
        trigger_type=req.trigger_type,
        triggered_by=user,
        start_time=datetime.now(timezone.utc),
        created_at=datetime.now(timezone.utc),
    )
    db.add(run)

    background_tasks.add_task(audit.run_started, pipeline_id, airflow_run_id, req.trigger_type, user)
    background_tasks.add_task(ws_manager.broadcast, {
        "type": "run_started",
        "pipeline_id": pipeline_id,
        "run_id": airflow_run_id,
    })

    return {"run_id": airflow_run_id, "status": "queued"}


@app.get("/api/runs", tags=["Runs"])
async def list_runs(
    pipeline_id: Optional[str] = None,
    status_filter: Optional[str] = Query(None, alias="status"),
    page: int = 1,
    page_size: int = 50,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    q = select(PipelineRun).order_by(desc(PipelineRun.created_at))
    if pipeline_id:
        q = q.where(PipelineRun.pipeline_id == pipeline_id)
    if status_filter:
        rs = _to_enum(RunStatus, status_filter)
        if rs:
            q = q.where(PipelineRun.status == rs)
    q = q.offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(q)
    runs = result.scalars().all()
    return {"runs": [
        {
            "run_id": r.run_id,
            "pipeline_id": r.pipeline_id,
            "status": r.status.value,
            "trigger_type": r.trigger_type,
            "triggered_by": r.triggered_by,
            "start_time": r.start_time.isoformat() if r.start_time else None,
            "end_time": r.end_time.isoformat() if r.end_time else None,
            "duration_seconds": r.duration_seconds,
            "total_tasks": r.total_tasks,
            "completed_tasks": r.completed_tasks,
            "failed_tasks": r.failed_tasks,
            "total_rows_processed": r.total_rows_processed,
        }
        for r in runs
    ]}


@app.get("/api/runs/{run_id}", tags=["Runs"])
async def get_run(
    run_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    result = await db.execute(select(PipelineRun).where(PipelineRun.run_id == run_id))
    run = result.scalar_one_or_none()
    if not run:
        raise HTTPException(404, f"Run '{run_id}' not found")

    tasks_result = await db.execute(
        select(TaskRun).where(TaskRun.run_id == run_id).order_by(TaskRun.created_at)
    )
    tasks = tasks_result.scalars().all()

    return {
        "run_id": run.run_id,
        "pipeline_id": run.pipeline_id,
        "status": run.status.value,
        "start_time": run.start_time.isoformat() if run.start_time else None,
        "end_time": run.end_time.isoformat() if run.end_time else None,
        "duration_seconds": run.duration_seconds,
        "total_rows_processed": run.total_rows_processed,
        "tasks": [
            {
                "task_run_id": t.task_run_id,
                "task_id": t.task_id,
                "task_type": t.task_type.value if t.task_type else None,
                "status": t.status.value,
                "start_time": t.start_time.isoformat() if t.start_time else None,
                "end_time": t.end_time.isoformat() if t.end_time else None,
                "duration_seconds": t.duration_seconds,
                "output_duckdb_path": t.output_duckdb_path,
                "output_row_count": t.output_row_count,
                "input_sources": t.input_sources,
                "qc_passed": t.qc_passed,
                "qc_results": t.qc_results,
                "qc_failures": t.qc_failures,
                "qc_warnings": t.qc_warnings,
                "error_message": t.error_message,
                "attempt_number": t.attempt_number,
                "eks_job_name": t.eks_job_name,
            }
            for t in tasks
        ],
    }


@app.post("/api/runs/{run_id}/rerun", tags=["Runs"])
async def rerun_task(
    run_id: str,
    req: RerunTaskRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    user = current_user.get("sub", "api")

    run_result = await db.execute(select(PipelineRun).where(PipelineRun.run_id == run_id))
    run = run_result.scalar_one_or_none()
    if not run:
        raise HTTPException(404, "Run not found")

    # Clear task status to allow rerun
    task_result = await db.execute(
        select(TaskRun)
        .where(TaskRun.run_id == run_id, TaskRun.task_id == req.task_id)
        .order_by(desc(TaskRun.created_at))
        .limit(1)
    )
    task = task_result.scalar_one_or_none()

    # Trigger Airflow task instance clear
    try:
        if req.mode == "downstream":
            await airflow_request(
                "POST",
                f"/dags/{run.pipeline_id}/clearTaskInstances",
                json={
                    "dag_run_id": run_id,
                    "task_ids": [req.task_id],
                    "include_downstream": True,
                    "include_upstream": False,
                    "reset_dag_runs": True,
                }
            )
        elif req.mode == "single":
            await airflow_request(
                "POST",
                f"/dags/{run.pipeline_id}/clearTaskInstances",
                json={"dag_run_id": run_id, "task_ids": [req.task_id]}
            )
        elif req.mode == "full":
            await airflow_request(
                "DELETE",
                f"/dags/{run.pipeline_id}/dagRuns/{run_id}"
            )
            await trigger_pipeline(
                run.pipeline_id,
                TriggerRunRequest(trigger_type="manual_rerun"),
                background_tasks, db, current_user
            )
    except Exception as e:
        logger.warning(f"Airflow rerun API failed: {e}")

    background_tasks.add_task(
        audit.task_rerun_requested,
        run.pipeline_id, run_id, req.task_id, user, req.reason, req.mode
    )

    return {"rerun_submitted": True, "task_id": req.task_id, "mode": req.mode}


# ─────────────────────────────────────────────────────────────────────────────
# ── TASKS ─────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/tasks", tags=["Tasks"])
async def list_task_runs(
    pipeline_id: Optional[str] = None,
    run_id: Optional[str] = None,
    status_filter: Optional[str] = Query(None, alias="status"),
    page: int = 1,
    page_size: int = 100,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    q = select(TaskRun).order_by(desc(TaskRun.created_at))
    if pipeline_id: q = q.where(TaskRun.pipeline_id == pipeline_id)
    if run_id:      q = q.where(TaskRun.run_id == run_id)
    if status_filter:
        ts = _to_enum(TaskStatus, status_filter)
        if ts:
            q = q.where(TaskRun.status == ts)
    q = q.offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(q)
    tasks = result.scalars().all()
    return {"tasks": [
        {
            "task_run_id": t.task_run_id,
            "task_id": t.task_id,
            "pipeline_id": t.pipeline_id,
            "run_id": t.run_id,
            "task_type": t.task_type.value if t.task_type else None,
            "status": t.status.value,
            "start_time": t.start_time.isoformat() if t.start_time else None,
            "end_time": t.end_time.isoformat() if t.end_time else None,
            "duration_seconds": t.duration_seconds,
            "output_duckdb_path": t.output_duckdb_path,
            "output_row_count": t.output_row_count,
            "output_size_bytes": t.output_size_bytes,
            "input_sources": t.input_sources,
            "qc_passed": t.qc_passed,
            "qc_warnings": t.qc_warnings,
            "qc_failures": t.qc_failures,
            "error_message": t.error_message,
            "attempt_number": t.attempt_number,
            "eks_job_name": t.eks_job_name,
            "metrics": t.metrics,
        }
        for t in tasks
    ]}


# ─────────────────────────────────────────────────────────────────────────────
# ── AUDIT ─────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/audit", tags=["Audit"])
async def get_audit_logs(
    pipeline_id: Optional[str] = None,
    run_id: Optional[str] = None,
    event_type: Optional[str] = None,
    user: Optional[str] = None,
    severity: Optional[str] = None,
    page: int = 1,
    page_size: int = 100,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    q = select(AuditLog).order_by(desc(AuditLog.timestamp))
    if pipeline_id: q = q.where(AuditLog.pipeline_id == pipeline_id)
    if run_id:      q = q.where(AuditLog.run_id == run_id)
    if event_type:  q = q.where(AuditLog.event_type == event_type)
    if user:        q = q.where(AuditLog.user == user)
    if severity:    q = q.where(AuditLog.severity == severity)
    q = q.offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(q)
    logs = result.scalars().all()
    return {"logs": [
        {
            "id": l.id,
            "event_type": l.event_type.value,
            "pipeline_id": l.pipeline_id,
            "run_id": l.run_id,
            "task_id": l.task_id,
            "user": l.user,
            "severity": l.severity.value,
            "details": l.details,
            "old_value": l.old_value,
            "new_value": l.new_value,
            "timestamp": l.timestamp.isoformat() if l.timestamp else None,
        }
        for l in logs
    ]}


# ─────────────────────────────────────────────────────────────────────────────
# ── ALERTS ────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/alerts", tags=["Alerts"])
async def list_alerts(
    pipeline_id: Optional[str] = None,
    resolved: Optional[bool] = None,
    severity: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    q = select(Alert).order_by(desc(Alert.fired_at))
    if pipeline_id:      q = q.where(Alert.pipeline_id == pipeline_id)
    if resolved is not None: q = q.where(Alert.is_resolved == resolved)
    if severity:         q = q.where(Alert.severity == severity)
    q = q.offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(q)
    alerts = result.scalars().all()
    return {"alerts": [
        {
            "id": a.id, "alert_type": a.alert_type, "severity": a.severity.value,
            "title": a.title, "message": a.message, "pipeline_id": a.pipeline_id,
            "run_id": a.run_id, "task_id": a.task_id, "details": a.details,
            "is_resolved": a.is_resolved, "resolved_by": a.resolved_by,
            "fired_at": a.fired_at.isoformat() if a.fired_at else None,
        }
        for a in alerts
    ]}


@app.post("/api/alerts/{alert_id}/resolve", tags=["Alerts"])
async def resolve_alert(
    alert_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    result = await db.execute(select(Alert).where(Alert.id == alert_id))
    alert = result.scalar_one_or_none()
    if not alert:
        raise HTTPException(404, "Alert not found")
    alert.is_resolved = True
    alert.resolved_at = datetime.now(timezone.utc)
    alert.resolved_by = current_user.get("sub", "api")
    return {"resolved": True}


# ─────────────────────────────────────────────────────────────────────────────
# ── CONNECTIONS ───────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/connections", tags=["Connections"])
async def list_connections(
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    result = await db.execute(select(DataConnection).where(DataConnection.is_active == True))
    conns = result.scalars().all()
    return {"connections": [
        {
            "id": c.id, "connection_id": c.connection_id, "name": c.name,
            "connection_type": c.connection_type.value, "host": c.host, "port": c.port,
            "database": c.database, "username": c.username,
            "is_active": c.is_active,
            "extra_config": c.extra_config or {},
            "last_tested_at": c.last_tested_at.isoformat() if c.last_tested_at else None,
            "last_test_success": c.last_test_success,
        }
        for c in conns
    ]}


@app.post("/api/connections", status_code=201, tags=["Connections"])
async def create_connection(
    req: ConnectionCreateRequest,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    conn = DataConnection(
        id=str(uuid.uuid4()),
        connection_id=req.connection_id,
        name=req.name,
        connection_type=ConnectionType(req.connection_type),
        host=req.host,
        port=req.port,
        database=req.database,
        schema_name=req.schema_name,
        username=req.username,
        extra_config=req.extra_config,
        created_by=current_user.get("sub", "api"),
    )
    db.add(conn)

    # Also register in Airflow
    try:
        await airflow_request("POST", "/connections", json={
            "connection_id": req.connection_id,
            "conn_type": req.connection_type,
            "host": req.host,
            "login": req.username,
            "password": req.password,
            "port": req.port,
            "schema": req.database,
        })
    except Exception as e:
        logger.warning(f"Airflow connection registration failed: {e}")

    return {"connection_id": req.connection_id, "created": True}


@app.post("/api/connections/{connection_id}/test", tags=["Connections"])
async def test_connection(
    connection_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    result = await db.execute(
        select(DataConnection).where(DataConnection.connection_id == connection_id)
    )
    conn = result.scalar_one_or_none()
    if not conn:
        raise HTTPException(404, "Connection not found")

    success = False
    error   = None
    try:
        conn_type = conn.connection_type.value
        if conn_type in ("sqlserver", "oracle", "postgresql", "mysql"):
            # Quick connectivity test
            import socket
            sock = socket.create_connection((conn.host, conn.port or 5432), timeout=5)
            sock.close()
            success = True
        elif conn_type == "s3":
            import boto3
            s3 = boto3.client("s3", endpoint_url=os.getenv("AWS_ENDPOINT_URL"))
            s3.list_buckets()
            success = True
        else:
            success = True
    except Exception as e:
        error = str(e)

    conn.last_tested_at    = datetime.now(timezone.utc)
    conn.last_test_success = success
    conn.last_test_error   = error

    return {"success": success, "error": error}


# ─────────────────────────────────────────────────────────────────────────────
# ── DEPLOYMENTS ───────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/deployments", tags=["Deployments"])
async def list_deployments(
    pipeline_id: Optional[str] = None,
    status_filter: Optional[str] = Query(None, alias="status"),
    environment: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    q = select(Deployment).order_by(desc(Deployment.submitted_at))
    if pipeline_id:   q = q.where(Deployment.pipeline_id == pipeline_id)
    if status_filter:
        ds = _to_enum(DeploymentStatus, status_filter)
        if ds:
            q = q.where(Deployment.status == ds)
    if environment:
        de = _to_enum(DeploymentEnvironment, environment)
        if de:
            q = q.where(Deployment.environment == de)
    q = q.limit(100)
    result = await db.execute(q)
    deps = result.scalars().all()
    return {"deployments": [
        {
            "id": d.id, "deployment_number": d.deployment_number,
            "pipeline_id": d.pipeline_id, "version": d.version,
            "deployment_type": d.deployment_type, "environment": d.environment.value,
            "status": d.status.value, "change_description": d.change_description,
            "submitted_by": d.submitted_by,
            "submitted_at": d.submitted_at.isoformat() if d.submitted_at else None,
            "approved_by": d.approved_by,
            "approved_at": d.approved_at.isoformat() if d.approved_at else None,
            "deployed_at": d.deployed_at.isoformat() if d.deployed_at else None,
        }
        for d in deps
    ]}


@app.post("/api/deployments", status_code=201, tags=["Deployments"])
async def create_deployment(
    req: DeploymentCreateRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    user = current_user.get("sub", "api")

    # Get next deployment number
    count_result = await db.execute(select(func.count(Deployment.id)))
    count = count_result.scalar() or 0
    dep_num = count + 1

    # Generate approval token
    approval_token = secrets.token_urlsafe(32)

    deployment = Deployment(
        id=str(uuid.uuid4()),
        deployment_number=dep_num,
        pipeline_id=req.pipeline_id,
        version=req.version,
        deployment_type=req.deployment_type,
        environment=DeploymentEnvironment(req.environment),
        status=DeploymentStatus.PENDING_APPROVAL,
        change_description=req.change_description,
        submitted_by=user,
        approver_email=req.approver_email,
        approval_token=approval_token,
        container_image=req.container_image,
    )
    db.add(deployment)
    await db.flush()

    # Send approval email
    if req.approver_email:
        background_tasks.add_task(
            _send_approval_email,
            req.approver_email,
            req.pipeline_id,
            req.version,
            dep_num,
            approval_token,
            user,
            req.change_description or "",
        )

    background_tasks.add_task(
        audit.deployment_submitted, str(deployment.id), req.pipeline_id, req.version, user
    )

    return {
        "deployment_id": deployment.id,
        "deployment_number": dep_num,
        "status": "pending_approval",
        "approval_token": approval_token,
    }


@app.post("/api/deployments/{deployment_id}/approve", tags=["Deployments"])
async def approve_deployment(
    deployment_id: str,
    background_tasks: BackgroundTasks,
    token: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    user = current_user.get("sub", "api")
    result = await db.execute(select(Deployment).where(Deployment.id == deployment_id))
    dep = result.scalar_one_or_none()
    if not dep:
        raise HTTPException(404, "Deployment not found")

    # Validate approval token if provided
    if token and dep.approval_token and token != dep.approval_token:
        raise HTTPException(403, "Invalid approval token")

    dep.status      = DeploymentStatus.APPROVED
    dep.approved_by = user
    dep.approved_at = datetime.now(timezone.utc)

    background_tasks.add_task(audit.deployment_approved, deployment_id, dep.pipeline_id, user)
    background_tasks.add_task(_execute_deployment, deployment_id, dep.pipeline_id, dep.version, dep.environment.value)

    return {"approved": True, "deployment_number": dep.deployment_number}


@app.post("/api/deployments/{deployment_id}/reject", tags=["Deployments"])
async def reject_deployment(
    deployment_id: str,
    reason: str = Body(..., embed=True),
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    result = await db.execute(select(Deployment).where(Deployment.id == deployment_id))
    dep = result.scalar_one_or_none()
    if not dep:
        raise HTTPException(404, "Deployment not found")
    dep.status = DeploymentStatus.REJECTED
    dep.rejection_reason = reason
    dep.approved_by = current_user.get("sub", "api")
    return {"rejected": True}


# ─────────────────────────────────────────────────────────────────────────────
# ── QUERY EXPLORER ────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
@app.post("/api/query", tags=["Query"])
async def query_duckdb(
    req: QueryRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    """
    Execute SQL against any DuckDB file from any pipeline run.
    Supports querying intermediate and final DuckDB outputs.
    """
    import boto3
    import duckdb as ddb

    user = current_user.get("sub", "api")
    start = datetime.now(timezone.utc)

    # Download DuckDB from S3
    s3_path    = req.duckdb_path
    local_path = f"/tmp/query_{uuid.uuid4().hex[:8]}.duckdb"

    try:
        s3_client_kwargs = dict(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        )
        endpoint = os.getenv("AWS_ENDPOINT_URL")
        if endpoint:
            s3_client_kwargs["endpoint_url"] = endpoint

        s3 = boto3.client("s3", **s3_client_kwargs)
        bucket, key = s3_path.replace("s3://", "").split("/", 1)
        s3.download_file(bucket, key, local_path)

        # Execute query with row limit
        conn = ddb.connect(local_path, read_only=True)

        # Wrap with limit for safety
        safe_sql = req.sql.strip().rstrip(";")
        if not safe_sql.upper().startswith("SELECT"):
            raise HTTPException(400, "Only SELECT queries are allowed in Query Explorer")

        limited_sql = f"SELECT * FROM ({safe_sql}) _q LIMIT {req.limit}"
        result = conn.execute(limited_sql)
        columns = [desc[0] for desc in result.description]
        rows    = result.fetchall()
        conn.close()

        duration = (datetime.now(timezone.utc) - start).total_seconds()

        # Log query in audit
        background_tasks.add_task(
            audit.query_executed,
            user, req.pipeline_id, req.run_id, req.task_id,
            s3_path, req.sql[:200], len(rows),
        )

        return {
            "columns": columns,
            "rows": [list(row) for row in rows],
            "row_count": len(rows),
            "duration_ms": round(duration * 1000, 1),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Query failed: {str(e)}")
    finally:
        if os.path.exists(local_path):
            os.unlink(local_path)


@app.get("/api/query/duckdb-files", tags=["Query"])
async def list_duckdb_files(
    pipeline_id: Optional[str] = None,
    run_id: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    # Source from task_runs — the duckdb_files registry is populated by operators
    # that don't exist yet, but task_runs is written by every EKS job on completion.
    q = (
        select(TaskRun)
        .where(TaskRun.output_duckdb_path.isnot(None))
        .where(TaskRun.status == "success")
        .order_by(desc(TaskRun.created_at))
    )
    if pipeline_id: q = q.where(TaskRun.pipeline_id == pipeline_id)
    if run_id:      q = q.where(TaskRun.run_id == run_id)
    q = q.limit(500)
    result = await db.execute(q)
    rows = result.scalars().all()
    return {"files": [
        {
            "s3_path":     t.output_duckdb_path,
            "pipeline_id": t.pipeline_id,
            "run_id":      t.run_id,
            "task_id":     t.task_id,
            "table_name":  t.output_table,
            "row_count":   t.output_row_count,
            "size_bytes":  t.output_size_bytes,
            "created_at":  t.created_at.isoformat() if t.created_at else None,
        }
        for t in rows
    ]}


# ─────────────────────────────────────────────────────────────────────────────
# ── METRICS ───────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/metrics/dashboard", tags=["Metrics"])
async def dashboard_metrics(
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # Active runs
    active_runs = await db.execute(
        select(func.count(PipelineRun.id)).where(
            PipelineRun.status.in_([RunStatus.RUNNING, RunStatus.QUEUED])
        )
    )

    # Today's runs
    today_runs = await db.execute(
        select(func.count(PipelineRun.id)).where(PipelineRun.created_at >= today_start)
    )
    today_success = await db.execute(
        select(func.count(PipelineRun.id)).where(
            PipelineRun.created_at >= today_start,
            PipelineRun.status == RunStatus.SUCCESS,
        )
    )
    today_failed = await db.execute(
        select(func.count(PipelineRun.id)).where(
            PipelineRun.created_at >= today_start,
            PipelineRun.status == RunStatus.FAILED,
        )
    )

    # Avg duration (today)
    avg_dur = await db.execute(
        select(func.avg(PipelineRun.duration_seconds)).where(
            PipelineRun.created_at >= today_start,
            PipelineRun.status == RunStatus.SUCCESS,
        )
    )

    # Rows processed today
    rows_today = await db.execute(
        select(func.sum(PipelineRun.total_rows_processed)).where(
            PipelineRun.created_at >= today_start
        )
    )

    # Active alerts
    active_alerts = await db.execute(
        select(func.count(Alert.id)).where(Alert.is_resolved == False)
    )
    failed_tasks_today = await db.execute(
        select(func.count(TaskRun.id)).where(
            TaskRun.created_at >= today_start,
            TaskRun.status == TaskStatus.FAILED,
        )
    )

    total_r = today_runs.scalar() or 0
    success_r = today_success.scalar() or 0
    success_rate = round((success_r / total_r * 100), 1) if total_r > 0 else 0

    return {
        "active_runs": active_runs.scalar() or 0,
        "today_runs": total_r,
        "today_success": success_r,
        "today_failed": today_failed.scalar() or 0,
        "success_rate": success_rate,
        "avg_duration_seconds": round(avg_dur.scalar() or 0, 1),
        "rows_today": rows_today.scalar() or 0,
        "active_alerts": active_alerts.scalar() or 0,
        "failed_tasks_today": failed_tasks_today.scalar() or 0,
    }


@app.get("/api/metrics/throughput", tags=["Metrics"])
async def throughput_metrics(
    hours: int = 24,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    """Hourly throughput for the last N hours"""
    # This would ideally use time_bucket / generate_series in Postgres
    # For simplicity, return sample data structure
    from sqlalchemy import text
    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    result = await db.execute(text("""
        SELECT
            date_trunc('hour', start_time) as hour,
            COUNT(*) as run_count,
            SUM(total_rows_processed) as rows_processed,
            AVG(duration_seconds) as avg_duration
        FROM pipeline_runs
        WHERE start_time >= :since
        GROUP BY date_trunc('hour', start_time)
        ORDER BY hour
    """), {"since": since})

    rows = result.fetchall()
    return {
        "hourly": [
            {
                "hour": row[0].isoformat() if row[0] else None,
                "run_count": row[1],
                "rows_processed": row[2] or 0,
                "avg_duration": round(row[3] or 0, 1),
            }
            for row in rows
        ]
    }


# ─────────────────────────────────────────────────────────────────────────────
# ── EKS JOBS ──────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/eks/jobs", tags=["EKS"])
async def list_eks_jobs(
    pipeline_id: Optional[str] = None,
    status_filter: Optional[str] = Query(None, alias="status"),
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(maybe_token),
):
    q = select(EKSJob).order_by(desc(EKSJob.submitted_at))
    if pipeline_id:   q = q.where(EKSJob.pipeline_id == pipeline_id)
    if status_filter: q = q.where(EKSJob.status == status_filter)
    q = q.limit(100)
    result = await db.execute(q)
    jobs = result.scalars().all()
    return {"jobs": [
        {
            "job_name": j.job_name, "pipeline_id": j.pipeline_id,
            "task_id": j.task_id, "image": j.image,
            "cpu_request": j.cpu_request, "memory_request": j.memory_request,
            "status": j.status, "pod_name": j.pod_name, "node_name": j.node_name,
            "submitted_at": j.submitted_at.isoformat() if j.submitted_at else None,
            "started_at": j.started_at.isoformat() if j.started_at else None,
            "completed_at": j.completed_at.isoformat() if j.completed_at else None,
        }
        for j in jobs
    ]}


# ─────────────────────────────────────────────────────────────────────────────
# ── WEBSOCKET ─────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            # Echo back pings / process client messages
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)


# ─────────────────────────────────────────────────────────────────────────────
# ── HEALTH ────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/health", tags=["System"])
async def health():
    return {"status": "ok", "version": "2.4.0", "time": datetime.now(timezone.utc).isoformat()}

@app.get("/api/event-types", tags=["System"])
async def event_types():
    return {"event_types": [e.value for e in AuditEventType]}


# ─────────────────────────────────────────────────────────────────────────────
# Background helpers
# ─────────────────────────────────────────────────────────────────────────────
async def _sync_dag_to_airflow(pipeline_id: str, config: dict):
    """Upload pipeline config to S3 so Airflow picks it up on next scan"""
    try:
        import boto3
        import json as json_lib
        endpoint = os.getenv("AWS_ENDPOINT_URL")
        kwargs   = dict(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        )
        if endpoint:
            kwargs["endpoint_url"] = endpoint
        s3 = boto3.client("s3", **kwargs)
        bucket = os.getenv("NEXTGEN_DATABRIDGE_PIPELINE_CONFIGS_BUCKET", "nextgen-databridge-pipeline-configs")
        key    = f"active/{pipeline_id}.json"
        s3.put_object(Bucket=bucket, Key=key, Body=json_lib.dumps(config).encode())
        logger.info(f"Synced config for {pipeline_id} to S3")
    except Exception as e:
        logger.error(f"S3 config sync failed: {e}")


async def _execute_deployment(deployment_id: str, pipeline_id: str, version: str, environment: str):
    """Execute an approved deployment"""
    import asyncio
    from models.database import AsyncSessionLocal
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(Deployment).where(Deployment.id == deployment_id))
        dep = result.scalar_one_or_none()
        if not dep:
            return

        dep.status = DeploymentStatus.DEPLOYING
        dep.deployment_log = [{"time": datetime.now(timezone.utc).isoformat(), "msg": "Deployment started"}]
        await db.commit()

        try:
            # 1. Get active config
            config_result = await db.execute(
                select(PipelineConfig)
                .where(PipelineConfig.pipeline_id == pipeline_id, PipelineConfig.version == version)
            )
            config = config_result.scalar_one_or_none()
            if not config:
                raise ValueError(f"Config version {version} not found")

            # 2. Upload to S3
            await _sync_dag_to_airflow(pipeline_id, config.config)
            dep.deployment_log.append({"time": datetime.now(timezone.utc).isoformat(), "msg": "Config synced to S3"})

            # 3. Update pipeline record
            await db.execute(
                update(Pipeline)
                .where(Pipeline.pipeline_id == pipeline_id)
                .values(current_version=version)
            )
            dep.deployment_log.append({"time": datetime.now(timezone.utc).isoformat(), "msg": "Pipeline version updated"})

            # 4. Airflow DAG refresh — unpause if needed
            try:
                await airflow_request("PATCH", f"/dags/{pipeline_id}", json={"is_paused": False})
                dep.deployment_log.append({"time": datetime.now(timezone.utc).isoformat(), "msg": "Airflow DAG activated"})
            except Exception:
                pass

            dep.status      = DeploymentStatus.DEPLOYED
            dep.deployed_at = datetime.now(timezone.utc)
            dep.deployment_log.append({"time": datetime.now(timezone.utc).isoformat(), "msg": "Deployment complete"})

        except Exception as e:
            dep.status = DeploymentStatus.FAILED
            dep.deployment_log = dep.deployment_log or []
            dep.deployment_log.append({"time": datetime.now(timezone.utc).isoformat(), "msg": f"FAILED: {str(e)}"})
            logger.error(f"Deployment {deployment_id} failed: {e}")

        await db.commit()


async def _send_approval_email(
    recipient: str,
    pipeline_id: str,
    version: str,
    dep_num: int,
    token: str,
    submitted_by: str,
    description: str,
):
    """Send deployment approval email with approve/reject links"""
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    ui_url = os.getenv("NEXTGEN_DATABRIDGE_UI_URL", "http://localhost:3000")
    approve_url = f"{ui_url}/deployments/{dep_num}/approve?token={token}"
    reject_url  = f"{ui_url}/deployments/{dep_num}/reject?token={token}"

    subject = f"[NextGenDatabridge] Deployment #{dep_num} requires your approval: {pipeline_id} v{version}"
    html = f"""
<html><body>
<h2>NextGenDatabridge Deployment Approval Required</h2>
<table>
  <tr><td><b>Deployment #:</b></td><td>{dep_num}</td></tr>
  <tr><td><b>Pipeline:</b></td><td>{pipeline_id}</td></tr>
  <tr><td><b>Version:</b></td><td>{version}</td></tr>
  <tr><td><b>Submitted by:</b></td><td>{submitted_by}</td></tr>
  <tr><td><b>Change:</b></td><td>{description}</td></tr>
</table>
<br/>
<a href="{approve_url}" style="background:#28a745;color:white;padding:10px 20px;text-decoration:none;border-radius:4px">
  ✓ Approve Deployment
</a>
&nbsp;&nbsp;
<a href="{reject_url}" style="background:#dc3545;color:white;padding:10px 20px;text-decoration:none;border-radius:4px">
  ✗ Reject
</a>
<br/><br/>
<p>View in <a href="{ui_url}/deployments/{dep_num}">NextGenDatabridge UI</a></p>
</body></html>
    """

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = os.getenv("SMTP_FROM", "nextgen-databridge@platform.internal")
        msg["To"]      = recipient
        msg.attach(MIMEText(html, "html"))

        with smtplib.SMTP(
            os.getenv("SMTP_HOST", "mailhog"),
            int(os.getenv("SMTP_PORT", "1025")),
            timeout=10,
        ) as s:
            s.sendmail(msg["From"], [recipient], msg.as_string())
        logger.info(f"Approval email sent to {recipient}")
    except Exception as e:
        logger.error(f"Failed to send approval email: {e}")
