"""
NextGenDatabridge Platform - Database Models
Full SQLAlchemy async models for audit, config, pipeline, and task tracking
"""
from datetime import datetime
from typing import Optional, Dict, Any, List
from enum import Enum as PyEnum
import uuid

from sqlalchemy import (
    Column, String, Integer, BigInteger, Float, Boolean, DateTime,
    Text, JSON, Enum, ForeignKey, Index, UniqueConstraint, event
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.ext.asyncio import AsyncAttrs, create_async_engine, AsyncSession
from sqlalchemy.orm import DeclarativeBase, relationship, Mapped, mapped_column
from sqlalchemy.sql import func


def _enum_col(enum_cls, **kwargs):
    """Create an Enum column using enum VALUES (lowercase) not names (uppercase).
    Prevents create_all() from storing enum names as PostgreSQL labels."""
    return Enum(enum_cls, values_callable=lambda e: [m.value for m in e], **kwargs)


# ─────────────────────────────────────────────────────────────────────────────
# Base
# ─────────────────────────────────────────────────────────────────────────────
class Base(AsyncAttrs, DeclarativeBase):
    pass


# ─────────────────────────────────────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────────────────────────────────────
class PipelineStatus(PyEnum):
    ACTIVE = "active"
    PAUSED = "paused"
    DEPRECATED = "deprecated"
    DRAFT = "draft"

class RunStatus(PyEnum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    QUEUED = "queued"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"

class TaskStatus(PyEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    UPSTREAM_FAILED = "upstream_failed"
    RETRYING = "retrying"

class TaskType(PyEnum):
    SQL_EXTRACT = "sql_extract"
    SQL_TRANSFORM = "sql_transform"
    DUCKDB_TRANSFORM = "duckdb_transform"
    DUCKDB_QUERY = "duckdb_query"
    SCHEMA_VALIDATE = "schema_validate"
    DATA_QUALITY = "data_quality"
    CDC_EXTRACT = "cdc_extract"
    FILE_INGEST = "file_ingest"
    KAFKA_CONSUME = "kafka_consume"
    KAFKA_PRODUCE = "kafka_produce"
    PUBSUB_CONSUME = "pubsub_consume"
    PUBSUB_PUBLISH = "pubsub_publish"
    EKS_JOB = "eks_job"
    EKS_EXTRACT = "eks_extract"
    EKS_TRANSFORM = "eks_transform"
    PYTHON_CALLABLE = "python_callable"
    CONDITIONAL_BRANCH = "conditional_branch"
    LOAD_TARGET = "load_target"
    NOTIFICATION = "notification"
    API_CALL = "api_call"
    AUTOSYS_JOB = "autosys_job"
    STORED_PROC = "stored_proc"

class ConnectionType(PyEnum):
    SQLSERVER = "sqlserver"
    ORACLE = "oracle"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    KAFKA = "kafka"
    PUBSUB = "pubsub"
    S3 = "s3"
    GCS = "gcs"
    FILE = "file"
    HTTP = "http"
    REDIS = "redis"

class DeploymentStatus(PyEnum):
    DRAFT = "draft"
    PENDING_APPROVAL = "pending_approval"
    APPROVED = "approved"
    REJECTED = "rejected"
    DEPLOYING = "deploying"
    DEPLOYED = "deployed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"

class DeploymentEnvironment(PyEnum):
    DEV = "dev"
    STAGING = "staging"
    PRODUCTION = "production"

class AlertSeverity(PyEnum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

class AuditEventType(PyEnum):
    PIPELINE_CREATED = "PIPELINE_CREATED"
    PIPELINE_UPDATED = "PIPELINE_UPDATED"
    PIPELINE_DELETED = "PIPELINE_DELETED"
    PIPELINE_PAUSED = "PIPELINE_PAUSED"
    PIPELINE_RESUMED = "PIPELINE_RESUMED"
    RUN_STARTED = "RUN_STARTED"
    RUN_COMPLETED = "RUN_COMPLETED"
    RUN_FAILED = "RUN_FAILED"
    RUN_CANCELLED = "RUN_CANCELLED"
    TASK_STARTED = "TASK_STARTED"
    TASK_COMPLETED = "TASK_COMPLETED"
    TASK_FAILED = "TASK_FAILED"
    TASK_RETRIED = "TASK_RETRIED"
    TASK_SKIPPED = "TASK_SKIPPED"
    TASK_RERUN_REQUESTED = "TASK_RERUN_REQUESTED"
    CONFIG_UPDATED = "CONFIG_UPDATED"
    CONNECTION_CREATED = "CONNECTION_CREATED"
    CONNECTION_UPDATED = "CONNECTION_UPDATED"
    CONNECTION_DELETED = "CONNECTION_DELETED"
    DEPLOYMENT_SUBMITTED = "DEPLOYMENT_SUBMITTED"
    DEPLOYMENT_APPROVED = "DEPLOYMENT_APPROVED"
    DEPLOYMENT_REJECTED = "DEPLOYMENT_REJECTED"
    DEPLOYMENT_COMPLETED = "DEPLOYMENT_COMPLETED"
    DEPLOYMENT_FAILED = "DEPLOYMENT_FAILED"
    DEPLOYMENT_ROLLED_BACK = "DEPLOYMENT_ROLLED_BACK"
    ALERT_FIRED = "ALERT_FIRED"
    ALERT_RESOLVED = "ALERT_RESOLVED"
    USER_LOGIN = "USER_LOGIN"
    QUERY_EXECUTED = "QUERY_EXECUTED"
    SCHEDULE_UPDATED = "SCHEDULE_UPDATED"


# ─────────────────────────────────────────────────────────────────────────────
# Config Models
# ─────────────────────────────────────────────────────────────────────────────
class PipelineConfig(Base):
    """Stores versioned pipeline JSON configurations"""
    __tablename__ = "pipeline_configs"

    id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid.uuid4()))
    pipeline_id: Mapped[str] = mapped_column(String(200), nullable=False, index=True)
    version: Mapped[str] = mapped_column(String(50), nullable=False)
    config: Mapped[dict] = mapped_column(JSONB, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(Boolean, default=False)
    is_valid: Mapped[bool] = mapped_column(Boolean, default=False)
    validation_errors: Mapped[Optional[dict]] = mapped_column(JSONB)
    created_by: Mapped[str] = mapped_column(String(200), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())
    deployed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    deployed_to: Mapped[Optional[str]] = mapped_column(String(50))

    __table_args__ = (
        UniqueConstraint('pipeline_id', 'version', name='uq_pipeline_version'),
        Index('ix_pipeline_configs_pipeline_id_active', 'pipeline_id', 'is_active'),
    )


class Pipeline(Base):
    """Pipeline registry — one row per logical pipeline"""
    __tablename__ = "pipelines"

    id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid.uuid4()))
    pipeline_id: Mapped[str] = mapped_column(String(200), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    status: Mapped[PipelineStatus] = mapped_column(_enum_col(PipelineStatus), default=PipelineStatus.ACTIVE)
    current_version: Mapped[Optional[str]] = mapped_column(String(50))
    schedule: Mapped[Optional[str]] = mapped_column(String(100))
    source_type: Mapped[Optional[str]] = mapped_column(String(100))
    target_type: Mapped[Optional[str]] = mapped_column(String(100))
    tags: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    owner: Mapped[Optional[str]] = mapped_column(String(200))
    team: Mapped[Optional[str]] = mapped_column(String(200))
    sla_minutes: Mapped[Optional[int]] = mapped_column(Integer)
    created_by: Mapped[str] = mapped_column(String(200), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())

    runs: Mapped[List["PipelineRun"]] = relationship("PipelineRun", back_populates="pipeline", lazy="select")


# ─────────────────────────────────────────────────────────────────────────────
# Runtime / Audit Models
# ─────────────────────────────────────────────────────────────────────────────
class PipelineRun(Base):
    """One row per DAG run execution"""
    __tablename__ = "pipeline_runs"

    id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid.uuid4()))
    run_id: Mapped[str] = mapped_column(String(500), unique=True, nullable=False, index=True)
    pipeline_id: Mapped[str] = mapped_column(String(200), ForeignKey("pipelines.pipeline_id"), nullable=False)
    pipeline_version: Mapped[Optional[str]] = mapped_column(String(50))
    airflow_dag_id: Mapped[Optional[str]] = mapped_column(String(500))
    airflow_run_id: Mapped[Optional[str]] = mapped_column(String(500))
    status: Mapped[RunStatus] = mapped_column(_enum_col(RunStatus), default=RunStatus.QUEUED, index=True)
    trigger_type: Mapped[str] = mapped_column(String(50), default="scheduled")  # scheduled, manual, api
    triggered_by: Mapped[Optional[str]] = mapped_column(String(200))
    config_snapshot: Mapped[Optional[dict]] = mapped_column(JSONB)  # Config at time of run
    start_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    end_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    duration_seconds: Mapped[Optional[float]] = mapped_column(Float)
    total_tasks: Mapped[int] = mapped_column(Integer, default=0)
    completed_tasks: Mapped[int] = mapped_column(Integer, default=0)
    failed_tasks: Mapped[int] = mapped_column(Integer, default=0)
    total_rows_processed: Mapped[int] = mapped_column(BigInteger, default=0)
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    run_metadata: Mapped[Optional[dict]] = mapped_column("metadata", JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    pipeline: Mapped["Pipeline"] = relationship("Pipeline", back_populates="runs")
    task_runs: Mapped[List["TaskRun"]] = relationship("TaskRun", back_populates="pipeline_run", lazy="select")

    __table_args__ = (
        Index('ix_pipeline_runs_pipeline_status', 'pipeline_id', 'status'),
        Index('ix_pipeline_runs_start_time', 'start_time'),
    )


class TaskRun(Base):
    """One row per task execution within a pipeline run. Core audit table."""
    __tablename__ = "task_runs"

    id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid.uuid4()))
    task_run_id: Mapped[str] = mapped_column(String(500), unique=True, nullable=False, index=True)
    run_id: Mapped[str] = mapped_column(String(500), ForeignKey("pipeline_runs.run_id"), nullable=False, index=True)
    pipeline_id: Mapped[str] = mapped_column(String(200), nullable=False, index=True)
    task_id: Mapped[str] = mapped_column(String(200), nullable=False)
    task_type: Mapped[TaskType] = mapped_column(_enum_col(TaskType), nullable=True)
    status: Mapped[TaskStatus] = mapped_column(_enum_col(TaskStatus), default=TaskStatus.PENDING)
    attempt_number: Mapped[int] = mapped_column(Integer, default=1)
    max_attempts: Mapped[int] = mapped_column(Integer, default=3)

    # Input / Output tracking
    input_sources: Mapped[Optional[dict]] = mapped_column(JSONB)  # [{type, connection, path, table, query}]
    output_duckdb_path: Mapped[Optional[str]] = mapped_column(Text)
    output_table: Mapped[Optional[str]] = mapped_column(String(500))
    output_row_count: Mapped[Optional[int]] = mapped_column(BigInteger)
    output_size_bytes: Mapped[Optional[int]] = mapped_column(BigInteger)
    output_schema: Mapped[Optional[dict]] = mapped_column(JSONB)

    # Timing
    queued_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    start_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    end_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    duration_seconds: Mapped[Optional[float]] = mapped_column(Float)

    # Execution context
    airflow_task_instance: Mapped[Optional[str]] = mapped_column(String(500))
    eks_job_name: Mapped[Optional[str]] = mapped_column(String(500))
    eks_pod_name: Mapped[Optional[str]] = mapped_column(String(500))
    worker_host: Mapped[Optional[str]] = mapped_column(String(500))

    # QC / Validation results
    qc_results: Mapped[Optional[dict]] = mapped_column(JSONB)
    qc_passed: Mapped[Optional[bool]] = mapped_column(Boolean)
    qc_warnings: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    qc_failures: Mapped[Optional[int]] = mapped_column(Integer, default=0)

    # Error info
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    error_traceback: Mapped[Optional[str]] = mapped_column(Text)

    # Task config at time of execution
    task_config_snapshot: Mapped[Optional[dict]] = mapped_column(JSONB)

    # Metrics
    metrics: Mapped[Optional[dict]] = mapped_column(JSONB)  # {cpu_seconds, memory_peak_mb, ...}
    logs_url: Mapped[Optional[str]] = mapped_column(Text)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())

    pipeline_run: Mapped["PipelineRun"] = relationship("PipelineRun", back_populates="task_runs")

    __table_args__ = (
        Index('ix_task_runs_run_pipeline', 'run_id', 'pipeline_id'),
        Index('ix_task_runs_status', 'status'),
        Index('ix_task_runs_start_time', 'start_time'),
    )


class AuditLog(Base):
    """Immutable audit trail for all platform events"""
    __tablename__ = "audit_logs"

    id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid.uuid4()))
    event_type: Mapped[AuditEventType] = mapped_column(_enum_col(AuditEventType), nullable=False, index=True)
    pipeline_id: Mapped[Optional[str]] = mapped_column(String(200), index=True)
    run_id: Mapped[Optional[str]] = mapped_column(String(500), index=True)
    task_id: Mapped[Optional[str]] = mapped_column(String(200))
    user: Mapped[Optional[str]] = mapped_column(String(200), index=True)
    ip_address: Mapped[Optional[str]] = mapped_column(String(50))
    details: Mapped[Optional[dict]] = mapped_column(JSONB)
    old_value: Mapped[Optional[dict]] = mapped_column(JSONB)
    new_value: Mapped[Optional[dict]] = mapped_column(JSONB)
    severity: Mapped[AlertSeverity] = mapped_column(_enum_col(AlertSeverity), default=AlertSeverity.INFO)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    correlation_id: Mapped[Optional[str]] = mapped_column(String(200))

    __table_args__ = (
        Index('ix_audit_logs_timestamp', 'timestamp'),
        Index('ix_audit_logs_event_pipeline', 'event_type', 'pipeline_id'),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Connection / Source Models
# ─────────────────────────────────────────────────────────────────────────────
class DataConnection(Base):
    """Source and target connection registry"""
    __tablename__ = "data_connections"

    id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid.uuid4()))
    connection_id: Mapped[str] = mapped_column(String(200), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    connection_type: Mapped[ConnectionType] = mapped_column(_enum_col(ConnectionType), nullable=False)
    host: Mapped[Optional[str]] = mapped_column(String(500))
    port: Mapped[Optional[int]] = mapped_column(Integer)
    database: Mapped[Optional[str]] = mapped_column(String(200))
    schema_name: Mapped[Optional[str]] = mapped_column(String(200))
    username: Mapped[Optional[str]] = mapped_column(String(200))
    # Password/credentials stored in AWS Secrets Manager — only secret ARN here
    secret_arn: Mapped[Optional[str]] = mapped_column(String(500))
    extra_config: Mapped[Optional[dict]] = mapped_column(JSONB)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    last_tested_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_test_success: Mapped[Optional[bool]] = mapped_column(Boolean)
    last_test_error: Mapped[Optional[str]] = mapped_column(Text)
    created_by: Mapped[str] = mapped_column(String(200), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())


# ─────────────────────────────────────────────────────────────────────────────
# Alert / Notification Models
# ─────────────────────────────────────────────────────────────────────────────
class Alert(Base):
    """Fired alerts"""
    __tablename__ = "alerts"

    id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid.uuid4()))
    alert_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    severity: Mapped[AlertSeverity] = mapped_column(_enum_col(AlertSeverity), nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    pipeline_id: Mapped[Optional[str]] = mapped_column(String(200), index=True)
    run_id: Mapped[Optional[str]] = mapped_column(String(500))
    task_id: Mapped[Optional[str]] = mapped_column(String(200))
    details: Mapped[Optional[dict]] = mapped_column(JSONB)
    is_resolved: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    resolved_by: Mapped[Optional[str]] = mapped_column(String(200))
    notified_channels: Mapped[Optional[list]] = mapped_column(JSONB)
    fired_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)


class AlertRule(Base):
    """Configurable alert rules per pipeline"""
    __tablename__ = "alert_rules"

    id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid.uuid4()))
    pipeline_id: Mapped[Optional[str]] = mapped_column(String(200), index=True)  # null = global
    rule_name: Mapped[str] = mapped_column(String(200), nullable=False)
    condition: Mapped[str] = mapped_column(String(100), nullable=False)  # task_failed, sla_breach, row_count_drop
    threshold: Mapped[Optional[dict]] = mapped_column(JSONB)
    severity: Mapped[AlertSeverity] = mapped_column(Enum(AlertSeverity), default=AlertSeverity.ERROR)
    channels: Mapped[list] = mapped_column(JSONB, default=list)  # [slack:#channel, email:addr, pagerduty:key]
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_by: Mapped[str] = mapped_column(String(200), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


# ─────────────────────────────────────────────────────────────────────────────
# Deployment Models
# ─────────────────────────────────────────────────────────────────────────────
class Deployment(Base):
    """Pipeline deployment with approval workflow"""
    __tablename__ = "deployments"

    id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid.uuid4()))
    deployment_number: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    pipeline_id: Mapped[str] = mapped_column(String(200), nullable=False, index=True)
    version: Mapped[str] = mapped_column(String(50), nullable=False)
    deployment_type: Mapped[str] = mapped_column(String(50), nullable=False)  # config, code, container, full
    environment: Mapped[DeploymentEnvironment] = mapped_column(_enum_col(DeploymentEnvironment), nullable=False)
    status: Mapped[DeploymentStatus] = mapped_column(_enum_col(DeploymentStatus), default=DeploymentStatus.DRAFT, index=True)
    change_description: Mapped[Optional[str]] = mapped_column(Text)
    config_diff: Mapped[Optional[dict]] = mapped_column(JSONB)
    container_image: Mapped[Optional[str]] = mapped_column(String(500))
    submitted_by: Mapped[str] = mapped_column(String(200), nullable=False)
    submitted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    approver_email: Mapped[Optional[str]] = mapped_column(String(200))
    approved_by: Mapped[Optional[str]] = mapped_column(String(200))
    approved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    rejection_reason: Mapped[Optional[str]] = mapped_column(Text)
    deployed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    deployment_log: Mapped[Optional[list]] = mapped_column(JSONB)
    rollback_version: Mapped[Optional[str]] = mapped_column(String(50))
    approval_token: Mapped[Optional[str]] = mapped_column(String(500))  # For email-link approval
    previous_version: Mapped[Optional[str]] = mapped_column(String(50))

    __table_args__ = (
        UniqueConstraint('deployment_number', name='uq_deployment_number'),
    )


# ─────────────────────────────────────────────────────────────────────────────
# DuckDB Registry
# ─────────────────────────────────────────────────────────────────────────────
class DuckDBFile(Base):
    """Registry of all DuckDB files produced by pipeline tasks"""
    __tablename__ = "duckdb_files"

    id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid.uuid4()))
    s3_path: Mapped[str] = mapped_column(Text, nullable=False, unique=True, index=True)
    pipeline_id: Mapped[str] = mapped_column(String(200), nullable=False, index=True)
    run_id: Mapped[str] = mapped_column(String(500), nullable=False, index=True)
    task_id: Mapped[str] = mapped_column(String(200), nullable=False)
    table_name: Mapped[Optional[str]] = mapped_column(String(200))
    schema: Mapped[Optional[dict]] = mapped_column(JSONB)
    row_count: Mapped[Optional[int]] = mapped_column(BigInteger)
    size_bytes: Mapped[Optional[int]] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    is_intermediate: Mapped[bool] = mapped_column(Boolean, default=True)
    file_metadata: Mapped[Optional[dict]] = mapped_column("metadata", JSONB)


# ─────────────────────────────────────────────────────────────────────────────
# EKS Job Models
# ─────────────────────────────────────────────────────────────────────────────
class EKSJob(Base):
    """EKS Kubernetes job tracking"""
    __tablename__ = "eks_jobs"

    id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid.uuid4()))
    job_name: Mapped[str] = mapped_column(String(500), unique=True, nullable=False, index=True)
    task_run_id: Mapped[Optional[str]] = mapped_column(String(500), ForeignKey("task_runs.task_run_id"))
    pipeline_id: Mapped[str] = mapped_column(String(200), nullable=False)
    run_id: Mapped[str] = mapped_column(String(500), nullable=False)
    task_id: Mapped[str] = mapped_column(String(200), nullable=False)
    image: Mapped[str] = mapped_column(String(500), nullable=False)
    namespace: Mapped[str] = mapped_column(String(200), default="nextgen-databridge-jobs")
    cpu_request: Mapped[Optional[str]] = mapped_column(String(20))
    memory_request: Mapped[Optional[str]] = mapped_column(String(20))
    cpu_limit: Mapped[Optional[str]] = mapped_column(String(20))
    memory_limit: Mapped[Optional[str]] = mapped_column(String(20))
    status: Mapped[str] = mapped_column(String(50), default="pending")
    pod_name: Mapped[Optional[str]] = mapped_column(String(500))
    node_name: Mapped[Optional[str]] = mapped_column(String(500))
    exit_code: Mapped[Optional[int]] = mapped_column(Integer)
    submitted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    env_vars: Mapped[Optional[dict]] = mapped_column(JSONB)
    manifest: Mapped[Optional[dict]] = mapped_column(JSONB)


# ─────────────────────────────────────────────────────────────────────────────
# Metrics
# ─────────────────────────────────────────────────────────────────────────────
class PipelineMetrics(Base):
    """Aggregated pipeline run metrics for dashboards"""
    __tablename__ = "pipeline_metrics"

    id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid.uuid4()))
    pipeline_id: Mapped[str] = mapped_column(String(200), nullable=False, index=True)
    metric_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    total_runs: Mapped[int] = mapped_column(Integer, default=0)
    successful_runs: Mapped[int] = mapped_column(Integer, default=0)
    failed_runs: Mapped[int] = mapped_column(Integer, default=0)
    avg_duration_seconds: Mapped[Optional[float]] = mapped_column(Float)
    p95_duration_seconds: Mapped[Optional[float]] = mapped_column(Float)
    total_rows_processed: Mapped[int] = mapped_column(BigInteger, default=0)
    sla_breaches: Mapped[int] = mapped_column(Integer, default=0)

    __table_args__ = (
        UniqueConstraint('pipeline_id', 'metric_date', name='uq_pipeline_metrics_date'),
    )
