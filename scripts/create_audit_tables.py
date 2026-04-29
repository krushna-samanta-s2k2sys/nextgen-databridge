#!/usr/bin/env python3
"""Create all NextGenDatabridge audit tables in the PostgreSQL RDS instance."""
import sqlalchemy as sa
from sqlalchemy import (
    Column, String, Integer, BigInteger, Float, Boolean, DateTime,
    Text, ForeignKey, Index, UniqueConstraint, MetaData,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB

meta = MetaData()

# ── Enums ──────────────────────────────────────────────────────────────────────
pipeline_status_enum = sa.Enum('active','paused','deprecated','draft', name='pipelinestatus', create_type=False)
run_status_enum      = sa.Enum('running','success','failed','queued','cancelled','skipped', name='runstatus', create_type=False)
task_status_enum     = sa.Enum('pending','running','success','failed','skipped','upstream_failed','retrying', name='taskstatus', create_type=False)
task_type_enum       = sa.Enum(
    'sql_extract','sql_transform','duckdb_transform','duckdb_query','schema_validate',
    'data_quality','cdc_extract','file_ingest','kafka_consume','kafka_produce',
    'pubsub_consume','pubsub_publish','eks_job','python_callable','conditional_branch',
    'load_target','notification',
    name='tasktype', create_type=False,
)
connection_type_enum = sa.Enum('sqlserver','oracle','postgresql','mysql','kafka','pubsub','s3','gcs','file','http','redis', name='connectiontype', create_type=False)
deploy_status_enum   = sa.Enum('draft','pending_approval','approved','rejected','deploying','deployed','failed','rolled_back', name='deploymentstatus', create_type=False)
deploy_env_enum      = sa.Enum('dev','staging','production', name='deploymentenvironment', create_type=False)
alert_severity_enum  = sa.Enum('info','warning','error','critical', name='alertseverity', create_type=False)
audit_event_enum     = sa.Enum(
    'PIPELINE_CREATED','PIPELINE_UPDATED','PIPELINE_DELETED','PIPELINE_PAUSED','PIPELINE_RESUMED',
    'RUN_STARTED','RUN_COMPLETED','RUN_FAILED','RUN_CANCELLED',
    'TASK_STARTED','TASK_COMPLETED','TASK_FAILED','TASK_RETRIED','TASK_SKIPPED','TASK_RERUN_REQUESTED',
    'CONFIG_UPDATED','CONNECTION_CREATED','CONNECTION_UPDATED','CONNECTION_DELETED',
    'DEPLOYMENT_SUBMITTED','DEPLOYMENT_APPROVED','DEPLOYMENT_REJECTED','DEPLOYMENT_COMPLETED',
    'DEPLOYMENT_FAILED','DEPLOYMENT_ROLLED_BACK',
    'ALERT_FIRED','ALERT_RESOLVED','USER_LOGIN','QUERY_EXECUTED','SCHEDULE_UPDATED',
    name='auditeventtype', create_type=False,
)

# ── Tables ─────────────────────────────────────────────────────────────────────
sa.Table('pipelines', meta,
    Column('id', UUID(as_uuid=False), primary_key=True),
    Column('pipeline_id', String(200), unique=True, nullable=False, index=True),
    Column('name', String(500), nullable=False),
    Column('description', Text),
    Column('status', pipeline_status_enum, default='active'),
    Column('current_version', String(50)),
    Column('schedule', String(100)),
    Column('source_type', String(100)),
    Column('target_type', String(100)),
    Column('tags', JSONB),
    Column('owner', String(200)),
    Column('team', String(200)),
    Column('sla_minutes', Integer),
    Column('created_by', String(200), nullable=False),
    Column('created_at', DateTime(timezone=True), server_default=sa.func.now()),
    Column('updated_at', DateTime(timezone=True), server_default=sa.func.now()),
)

sa.Table('pipeline_configs', meta,
    Column('id', UUID(as_uuid=False), primary_key=True),
    Column('pipeline_id', String(200), nullable=False, index=True),
    Column('version', String(50), nullable=False),
    Column('config', JSONB, nullable=False),
    Column('description', Text),
    Column('is_active', Boolean, default=False),
    Column('is_valid', Boolean, default=False),
    Column('validation_errors', JSONB),
    Column('created_by', String(200), nullable=False),
    Column('created_at', DateTime(timezone=True), server_default=sa.func.now()),
    Column('updated_at', DateTime(timezone=True), server_default=sa.func.now()),
    Column('deployed_at', DateTime(timezone=True)),
    Column('deployed_to', String(50)),
    UniqueConstraint('pipeline_id', 'version', name='uq_pipeline_version'),
    Index('ix_pipeline_configs_pipeline_id_active', 'pipeline_id', 'is_active'),
)

sa.Table('pipeline_runs', meta,
    Column('id', UUID(as_uuid=False), primary_key=True),
    Column('run_id', String(500), unique=True, nullable=False, index=True),
    Column('pipeline_id', String(200), ForeignKey('pipelines.pipeline_id'), nullable=False),
    Column('pipeline_version', String(50)),
    Column('airflow_dag_id', String(500)),
    Column('airflow_run_id', String(500)),
    Column('status', run_status_enum, default='queued', index=True),
    Column('trigger_type', String(50), default='scheduled'),
    Column('triggered_by', String(200)),
    Column('config_snapshot', JSONB),
    Column('start_time', DateTime(timezone=True)),
    Column('end_time', DateTime(timezone=True)),
    Column('duration_seconds', Float),
    Column('total_tasks', Integer, default=0),
    Column('completed_tasks', Integer, default=0),
    Column('failed_tasks', Integer, default=0),
    Column('total_rows_processed', BigInteger, default=0),
    Column('error_message', Text),
    Column('metadata', JSONB),
    Column('created_at', DateTime(timezone=True), server_default=sa.func.now()),
    Index('ix_pipeline_runs_pipeline_status', 'pipeline_id', 'status'),
    Index('ix_pipeline_runs_start_time', 'start_time'),
)

sa.Table('task_runs', meta,
    Column('id', UUID(as_uuid=False), primary_key=True),
    Column('task_run_id', String(500), unique=True, nullable=False, index=True),
    Column('run_id', String(500), ForeignKey('pipeline_runs.run_id'), nullable=False, index=True),
    Column('pipeline_id', String(200), nullable=False, index=True),
    Column('task_id', String(200), nullable=False),
    Column('task_type', task_type_enum),
    Column('status', task_status_enum, default='pending'),
    Column('attempt_number', Integer, default=1),
    Column('max_attempts', Integer, default=3),
    Column('input_sources', JSONB),
    Column('output_duckdb_path', Text),
    Column('output_table', String(500)),
    Column('output_row_count', BigInteger),
    Column('output_size_bytes', BigInteger),
    Column('output_schema', JSONB),
    Column('queued_at', DateTime(timezone=True)),
    Column('start_time', DateTime(timezone=True)),
    Column('end_time', DateTime(timezone=True)),
    Column('duration_seconds', Float),
    Column('airflow_task_instance', String(500)),
    Column('eks_job_name', String(500)),
    Column('eks_pod_name', String(500)),
    Column('worker_host', String(500)),
    Column('qc_results', JSONB),
    Column('qc_passed', Boolean),
    Column('qc_warnings', Integer, default=0),
    Column('qc_failures', Integer, default=0),
    Column('error_message', Text),
    Column('error_traceback', Text),
    Column('task_config_snapshot', JSONB),
    Column('metrics', JSONB),
    Column('logs_url', Text),
    Column('created_at', DateTime(timezone=True), server_default=sa.func.now()),
    Column('updated_at', DateTime(timezone=True), server_default=sa.func.now()),
    Index('ix_task_runs_run_pipeline', 'run_id', 'pipeline_id'),
    Index('ix_task_runs_status', 'status'),
    Index('ix_task_runs_start_time', 'start_time'),
)

sa.Table('audit_logs', meta,
    Column('id', UUID(as_uuid=False), primary_key=True),
    Column('event_type', audit_event_enum, nullable=False, index=True),
    Column('pipeline_id', String(200), index=True),
    Column('run_id', String(500), index=True),
    Column('task_id', String(200)),
    Column('user', String(200), index=True),
    Column('ip_address', String(50)),
    Column('details', JSONB),
    Column('old_value', JSONB),
    Column('new_value', JSONB),
    Column('severity', alert_severity_enum, default='info'),
    Column('timestamp', DateTime(timezone=True), server_default=sa.func.now()),
    Column('correlation_id', String(200)),
    Index('ix_audit_logs_timestamp', 'timestamp'),
    Index('ix_audit_logs_event_pipeline', 'event_type', 'pipeline_id'),
)

sa.Table('data_connections', meta,
    Column('id', UUID(as_uuid=False), primary_key=True),
    Column('connection_id', String(200), unique=True, nullable=False, index=True),
    Column('name', String(500), nullable=False),
    Column('connection_type', connection_type_enum, nullable=False),
    Column('host', String(500)),
    Column('port', Integer),
    Column('database', String(200)),
    Column('schema_name', String(200)),
    Column('username', String(200)),
    Column('secret_arn', String(500)),
    Column('extra_config', JSONB),
    Column('is_active', Boolean, default=True),
    Column('last_tested_at', DateTime(timezone=True)),
    Column('last_test_success', Boolean),
    Column('last_test_error', Text),
    Column('created_by', String(200), nullable=False),
    Column('created_at', DateTime(timezone=True), server_default=sa.func.now()),
    Column('updated_at', DateTime(timezone=True), server_default=sa.func.now()),
)

sa.Table('alerts', meta,
    Column('id', UUID(as_uuid=False), primary_key=True),
    Column('alert_type', String(100), nullable=False, index=True),
    Column('severity', alert_severity_enum, nullable=False, index=True),
    Column('title', String(500), nullable=False),
    Column('message', Text, nullable=False),
    Column('pipeline_id', String(200), index=True),
    Column('run_id', String(500)),
    Column('task_id', String(200)),
    Column('details', JSONB),
    Column('is_resolved', Boolean, default=False, index=True),
    Column('resolved_at', DateTime(timezone=True)),
    Column('resolved_by', String(200)),
    Column('notified_channels', JSONB),
    Column('fired_at', DateTime(timezone=True), server_default=sa.func.now(), index=True),
)

sa.Table('alert_rules', meta,
    Column('id', UUID(as_uuid=False), primary_key=True),
    Column('pipeline_id', String(200), index=True),
    Column('rule_name', String(200), nullable=False),
    Column('condition', String(100), nullable=False),
    Column('threshold', JSONB),
    Column('severity', alert_severity_enum, default='error'),
    Column('channels', JSONB),
    Column('is_active', Boolean, default=True),
    Column('created_by', String(200), nullable=False),
    Column('created_at', DateTime(timezone=True), server_default=sa.func.now()),
)

sa.Table('deployments', meta,
    Column('id', UUID(as_uuid=False), primary_key=True),
    Column('deployment_number', Integer, nullable=False, index=True),
    Column('pipeline_id', String(200), nullable=False, index=True),
    Column('version', String(50), nullable=False),
    Column('deployment_type', String(50), nullable=False),
    Column('environment', deploy_env_enum, nullable=False),
    Column('status', deploy_status_enum, default='draft', index=True),
    Column('change_description', Text),
    Column('config_diff', JSONB),
    Column('container_image', String(500)),
    Column('submitted_by', String(200), nullable=False),
    Column('submitted_at', DateTime(timezone=True), server_default=sa.func.now()),
    Column('approver_email', String(200)),
    Column('approved_by', String(200)),
    Column('approved_at', DateTime(timezone=True)),
    Column('rejection_reason', Text),
    Column('deployed_at', DateTime(timezone=True)),
    Column('deployment_log', JSONB),
    Column('rollback_version', String(50)),
    Column('approval_token', String(500)),
    Column('previous_version', String(50)),
    UniqueConstraint('deployment_number', name='uq_deployment_number'),
)

sa.Table('duckdb_files', meta,
    Column('id', UUID(as_uuid=False), primary_key=True),
    Column('s3_path', Text, nullable=False, unique=True, index=True),
    Column('pipeline_id', String(200), nullable=False, index=True),
    Column('run_id', String(500), nullable=False, index=True),
    Column('task_id', String(200), nullable=False),
    Column('table_name', String(200)),
    Column('schema', JSONB),
    Column('row_count', BigInteger),
    Column('size_bytes', BigInteger),
    Column('created_at', DateTime(timezone=True), server_default=sa.func.now(), index=True),
    Column('expires_at', DateTime(timezone=True)),
    Column('is_intermediate', Boolean, default=True),
    Column('metadata', JSONB),
)

sa.Table('eks_jobs', meta,
    Column('id', UUID(as_uuid=False), primary_key=True),
    Column('job_name', String(500), unique=True, nullable=False, index=True),
    Column('task_run_id', String(500), ForeignKey('task_runs.task_run_id')),
    Column('pipeline_id', String(200), nullable=False),
    Column('run_id', String(500), nullable=False),
    Column('task_id', String(200), nullable=False),
    Column('image', String(500), nullable=False),
    Column('namespace', String(200), default='nextgen-databridge-jobs'),
    Column('cpu_request', String(20)),
    Column('memory_request', String(20)),
    Column('cpu_limit', String(20)),
    Column('memory_limit', String(20)),
    Column('status', String(50), default='pending'),
    Column('pod_name', String(500)),
    Column('node_name', String(500)),
    Column('exit_code', Integer),
    Column('submitted_at', DateTime(timezone=True), server_default=sa.func.now()),
    Column('started_at', DateTime(timezone=True)),
    Column('completed_at', DateTime(timezone=True)),
    Column('env_vars', JSONB),
    Column('manifest', JSONB),
)

sa.Table('pipeline_metrics', meta,
    Column('id', UUID(as_uuid=False), primary_key=True),
    Column('pipeline_id', String(200), nullable=False, index=True),
    Column('metric_date', DateTime(timezone=True), nullable=False, index=True),
    Column('total_runs', Integer, default=0),
    Column('successful_runs', Integer, default=0),
    Column('failed_runs', Integer, default=0),
    Column('avg_duration_seconds', Float),
    Column('p95_duration_seconds', Float),
    Column('total_rows_processed', BigInteger, default=0),
    Column('sla_breaches', Integer, default=0),
    UniqueConstraint('pipeline_id', 'metric_date', name='uq_pipeline_metrics_date'),
)

# ── Create all enums then tables ───────────────────────────────────────────────
url    = 'postgresql+psycopg2://airflow:ActionDag!1@nextgen-databridge-postgres.cor0mwswyvz9.us-east-1.rds.amazonaws.com:5432/airflow'
engine = sa.create_engine(url, echo=False)

all_enums = [
    pipeline_status_enum, run_status_enum, task_status_enum, task_type_enum,
    connection_type_enum, deploy_status_enum, deploy_env_enum,
    alert_severity_enum, audit_event_enum,
]

with engine.begin() as conn:
    for e in all_enums:
        try:
            e.create(conn, checkfirst=True)
        except Exception as ex:
            print(f"  Enum {e.name}: {ex}")

meta.create_all(engine, checkfirst=True)
print('All audit tables created successfully.')
