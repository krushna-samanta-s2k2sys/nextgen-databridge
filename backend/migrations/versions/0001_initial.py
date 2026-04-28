"""Initial schema: all NextGenDatabridge tables

Revision ID: 0001_initial
Revises: 
Create Date: 2024-01-01 00:00:00.000000
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = '0001_initial'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # Create all enums first
    pipeline_status = postgresql.ENUM('active','paused','deprecated','draft', name='pipelinestatus')
    run_status      = postgresql.ENUM('running','success','failed','queued','cancelled','skipped', name='runstatus')
    task_status     = postgresql.ENUM('pending','running','success','failed','skipped','upstream_failed','retrying', name='taskstatus')
    task_type       = postgresql.ENUM(
        'sql_extract','sql_transform','duckdb_transform','duckdb_query',
        'schema_validate','data_quality','cdc_extract','file_ingest',
        'kafka_consume','kafka_produce','pubsub_consume','pubsub_publish',
        'eks_job','python_callable','conditional_branch','load_target','notification',
        name='tasktype')
    conn_type       = postgresql.ENUM('sqlserver','oracle','postgresql','mysql','kafka','pubsub','s3','gcs','file','http','redis', name='connectiontype')
    dep_status      = postgresql.ENUM('draft','pending_approval','approved','rejected','deploying','deployed','failed','rolled_back', name='deploymentstatus')
    dep_env         = postgresql.ENUM('dev','staging','production', name='deploymentenvironment')
    alert_severity  = postgresql.ENUM('info','warning','error','critical', name='alertseverity')
    audit_event     = postgresql.ENUM(
        'PIPELINE_CREATED','PIPELINE_UPDATED','PIPELINE_DELETED','PIPELINE_PAUSED','PIPELINE_RESUMED',
        'RUN_STARTED','RUN_COMPLETED','RUN_FAILED','RUN_CANCELLED',
        'TASK_STARTED','TASK_COMPLETED','TASK_FAILED','TASK_RETRIED','TASK_SKIPPED','TASK_RERUN_REQUESTED',
        'CONFIG_UPDATED','CONNECTION_CREATED','CONNECTION_UPDATED','CONNECTION_DELETED',
        'DEPLOYMENT_SUBMITTED','DEPLOYMENT_APPROVED','DEPLOYMENT_REJECTED','DEPLOYMENT_COMPLETED',
        'DEPLOYMENT_FAILED','DEPLOYMENT_ROLLED_BACK',
        'ALERT_FIRED','ALERT_RESOLVED','USER_LOGIN','QUERY_EXECUTED','SCHEDULE_UPDATED',
        name='auditeventtype')

    for enum in [pipeline_status, run_status, task_status, task_type, conn_type,
                 dep_status, dep_env, alert_severity, audit_event]:
        enum.create(op.get_bind(), checkfirst=True)

    op.create_table('pipelines',
        sa.Column('id', postgresql.UUID(as_uuid=False), primary_key=True),
        sa.Column('pipeline_id', sa.String(200), unique=True, nullable=False),
        sa.Column('name', sa.String(500), nullable=False),
        sa.Column('description', sa.Text),
        sa.Column('status', sa.Enum('active','paused','deprecated','draft', name='pipelinestatus'), default='active'),
        sa.Column('current_version', sa.String(50)),
        sa.Column('schedule', sa.String(100)),
        sa.Column('source_type', sa.String(100)),
        sa.Column('target_type', sa.String(100)),
        sa.Column('tags', postgresql.JSONB),
        sa.Column('owner', sa.String(200)),
        sa.Column('team', sa.String(200)),
        sa.Column('sla_minutes', sa.Integer),
        sa.Column('created_by', sa.String(200), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index('ix_pipelines_pipeline_id', 'pipelines', ['pipeline_id'])

    op.create_table('pipeline_configs',
        sa.Column('id', postgresql.UUID(as_uuid=False), primary_key=True),
        sa.Column('pipeline_id', sa.String(200), nullable=False),
        sa.Column('version', sa.String(50), nullable=False),
        sa.Column('config', postgresql.JSONB, nullable=False),
        sa.Column('description', sa.Text),
        sa.Column('is_active', sa.Boolean, default=False),
        sa.Column('is_valid', sa.Boolean, default=False),
        sa.Column('validation_errors', postgresql.JSONB),
        sa.Column('created_by', sa.String(200), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('deployed_at', sa.DateTime(timezone=True)),
        sa.Column('deployed_to', sa.String(50)),
        sa.UniqueConstraint('pipeline_id', 'version', name='uq_pipeline_version'),
    )
    op.create_index('ix_pipeline_configs_pipeline_id', 'pipeline_configs', ['pipeline_id'])

    op.create_table('pipeline_runs',
        sa.Column('id', postgresql.UUID(as_uuid=False), primary_key=True),
        sa.Column('run_id', sa.String(500), unique=True, nullable=False),
        sa.Column('pipeline_id', sa.String(200), sa.ForeignKey('pipelines.pipeline_id'), nullable=False),
        sa.Column('pipeline_version', sa.String(50)),
        sa.Column('airflow_dag_id', sa.String(500)),
        sa.Column('airflow_run_id', sa.String(500)),
        sa.Column('status', sa.Enum('running','success','failed','queued','cancelled','skipped', name='runstatus'), default='queued'),
        sa.Column('trigger_type', sa.String(50), default='scheduled'),
        sa.Column('triggered_by', sa.String(200)),
        sa.Column('config_snapshot', postgresql.JSONB),
        sa.Column('start_time', sa.DateTime(timezone=True)),
        sa.Column('end_time', sa.DateTime(timezone=True)),
        sa.Column('duration_seconds', sa.Float),
        sa.Column('total_tasks', sa.Integer, default=0),
        sa.Column('completed_tasks', sa.Integer, default=0),
        sa.Column('failed_tasks', sa.Integer, default=0),
        sa.Column('total_rows_processed', sa.BigInteger, default=0),
        sa.Column('error_message', sa.Text),
        sa.Column('metadata', postgresql.JSONB),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index('ix_pipeline_runs_pipeline_status', 'pipeline_runs', ['pipeline_id', 'status'])
    op.create_index('ix_pipeline_runs_start_time', 'pipeline_runs', ['start_time'])

    op.create_table('task_runs',
        sa.Column('id', postgresql.UUID(as_uuid=False), primary_key=True),
        sa.Column('task_run_id', sa.String(500), unique=True, nullable=False),
        sa.Column('run_id', sa.String(500), sa.ForeignKey('pipeline_runs.run_id'), nullable=False),
        sa.Column('pipeline_id', sa.String(200), nullable=False),
        sa.Column('task_id', sa.String(200), nullable=False),
        sa.Column('task_type', sa.Enum('sql_extract','sql_transform','duckdb_transform','duckdb_query','schema_validate','data_quality','cdc_extract','file_ingest','kafka_consume','kafka_produce','pubsub_consume','pubsub_publish','eks_job','python_callable','conditional_branch','load_target','notification', name='tasktype')),
        sa.Column('status', sa.Enum('pending','running','success','failed','skipped','upstream_failed','retrying', name='taskstatus'), default='pending'),
        sa.Column('attempt_number', sa.Integer, default=1),
        sa.Column('max_attempts', sa.Integer, default=3),
        sa.Column('input_sources', postgresql.JSONB),
        sa.Column('output_duckdb_path', sa.Text),
        sa.Column('output_table', sa.String(500)),
        sa.Column('output_row_count', sa.BigInteger),
        sa.Column('output_size_bytes', sa.BigInteger),
        sa.Column('output_schema', postgresql.JSONB),
        sa.Column('queued_at', sa.DateTime(timezone=True)),
        sa.Column('start_time', sa.DateTime(timezone=True)),
        sa.Column('end_time', sa.DateTime(timezone=True)),
        sa.Column('duration_seconds', sa.Float),
        sa.Column('airflow_task_instance', sa.String(500)),
        sa.Column('eks_job_name', sa.String(500)),
        sa.Column('eks_pod_name', sa.String(500)),
        sa.Column('worker_host', sa.String(500)),
        sa.Column('qc_results', postgresql.JSONB),
        sa.Column('qc_passed', sa.Boolean),
        sa.Column('qc_warnings', sa.Integer, default=0),
        sa.Column('qc_failures', sa.Integer, default=0),
        sa.Column('error_message', sa.Text),
        sa.Column('error_traceback', sa.Text),
        sa.Column('task_config_snapshot', postgresql.JSONB),
        sa.Column('metrics', postgresql.JSONB),
        sa.Column('logs_url', sa.Text),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index('ix_task_runs_run_pipeline', 'task_runs', ['run_id', 'pipeline_id'])
    op.create_index('ix_task_runs_status', 'task_runs', ['status'])

    op.create_table('audit_logs',
        sa.Column('id', postgresql.UUID(as_uuid=False), primary_key=True),
        sa.Column('event_type', sa.Enum('PIPELINE_CREATED','PIPELINE_UPDATED','PIPELINE_DELETED','PIPELINE_PAUSED','PIPELINE_RESUMED','RUN_STARTED','RUN_COMPLETED','RUN_FAILED','RUN_CANCELLED','TASK_STARTED','TASK_COMPLETED','TASK_FAILED','TASK_RETRIED','TASK_SKIPPED','TASK_RERUN_REQUESTED','CONFIG_UPDATED','CONNECTION_CREATED','CONNECTION_UPDATED','CONNECTION_DELETED','DEPLOYMENT_SUBMITTED','DEPLOYMENT_APPROVED','DEPLOYMENT_REJECTED','DEPLOYMENT_COMPLETED','DEPLOYMENT_FAILED','DEPLOYMENT_ROLLED_BACK','ALERT_FIRED','ALERT_RESOLVED','USER_LOGIN','QUERY_EXECUTED','SCHEDULE_UPDATED', name='auditeventtype'), nullable=False),
        sa.Column('pipeline_id', sa.String(200)),
        sa.Column('run_id', sa.String(500)),
        sa.Column('task_id', sa.String(200)),
        sa.Column('user', sa.String(200)),
        sa.Column('ip_address', sa.String(50)),
        sa.Column('details', postgresql.JSONB),
        sa.Column('old_value', postgresql.JSONB),
        sa.Column('new_value', postgresql.JSONB),
        sa.Column('severity', sa.Enum('info','warning','error','critical', name='alertseverity'), default='info'),
        sa.Column('timestamp', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('correlation_id', sa.String(200)),
    )
    op.create_index('ix_audit_logs_timestamp', 'audit_logs', ['timestamp'])
    op.create_index('ix_audit_logs_event_pipeline', 'audit_logs', ['event_type', 'pipeline_id'])

    op.create_table('data_connections',
        sa.Column('id', postgresql.UUID(as_uuid=False), primary_key=True),
        sa.Column('connection_id', sa.String(200), unique=True, nullable=False),
        sa.Column('name', sa.String(500), nullable=False),
        sa.Column('connection_type', sa.Enum('sqlserver','oracle','postgresql','mysql','kafka','pubsub','s3','gcs','file','http','redis', name='connectiontype'), nullable=False),
        sa.Column('host', sa.String(500)),
        sa.Column('port', sa.Integer),
        sa.Column('database', sa.String(200)),
        sa.Column('schema_name', sa.String(200)),
        sa.Column('username', sa.String(200)),
        sa.Column('secret_arn', sa.String(500)),
        sa.Column('extra_config', postgresql.JSONB),
        sa.Column('is_active', sa.Boolean, default=True),
        sa.Column('last_tested_at', sa.DateTime(timezone=True)),
        sa.Column('last_test_success', sa.Boolean),
        sa.Column('last_test_error', sa.Text),
        sa.Column('created_by', sa.String(200), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_table('alerts',
        sa.Column('id', postgresql.UUID(as_uuid=False), primary_key=True),
        sa.Column('alert_type', sa.String(100), nullable=False),
        sa.Column('severity', sa.Enum('info','warning','error','critical', name='alertseverity'), nullable=False),
        sa.Column('title', sa.String(500), nullable=False),
        sa.Column('message', sa.Text, nullable=False),
        sa.Column('pipeline_id', sa.String(200)),
        sa.Column('run_id', sa.String(500)),
        sa.Column('task_id', sa.String(200)),
        sa.Column('details', postgresql.JSONB),
        sa.Column('is_resolved', sa.Boolean, default=False),
        sa.Column('resolved_at', sa.DateTime(timezone=True)),
        sa.Column('resolved_by', sa.String(200)),
        sa.Column('notified_channels', postgresql.JSONB),
        sa.Column('fired_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index('ix_alerts_severity', 'alerts', ['severity'])
    op.create_index('ix_alerts_is_resolved', 'alerts', ['is_resolved'])

    op.create_table('alert_rules',
        sa.Column('id', postgresql.UUID(as_uuid=False), primary_key=True),
        sa.Column('pipeline_id', sa.String(200)),
        sa.Column('rule_name', sa.String(200), nullable=False),
        sa.Column('condition', sa.String(100), nullable=False),
        sa.Column('threshold', postgresql.JSONB),
        sa.Column('severity', sa.Enum('info','warning','error','critical', name='alertseverity'), default='error'),
        sa.Column('channels', postgresql.JSONB, default=list),
        sa.Column('is_active', sa.Boolean, default=True),
        sa.Column('created_by', sa.String(200), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_table('deployments',
        sa.Column('id', postgresql.UUID(as_uuid=False), primary_key=True),
        sa.Column('deployment_number', sa.Integer, nullable=False),
        sa.Column('pipeline_id', sa.String(200), nullable=False),
        sa.Column('version', sa.String(50), nullable=False),
        sa.Column('deployment_type', sa.String(50), nullable=False),
        sa.Column('environment', sa.Enum('dev','staging','production', name='deploymentenvironment'), nullable=False),
        sa.Column('status', sa.Enum('draft','pending_approval','approved','rejected','deploying','deployed','failed','rolled_back', name='deploymentstatus'), default='draft'),
        sa.Column('change_description', sa.Text),
        sa.Column('config_diff', postgresql.JSONB),
        sa.Column('container_image', sa.String(500)),
        sa.Column('submitted_by', sa.String(200), nullable=False),
        sa.Column('submitted_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('approver_email', sa.String(200)),
        sa.Column('approved_by', sa.String(200)),
        sa.Column('approved_at', sa.DateTime(timezone=True)),
        sa.Column('rejection_reason', sa.Text),
        sa.Column('deployed_at', sa.DateTime(timezone=True)),
        sa.Column('deployment_log', postgresql.JSONB),
        sa.Column('rollback_version', sa.String(50)),
        sa.Column('approval_token', sa.String(500)),
        sa.Column('previous_version', sa.String(50)),
        sa.UniqueConstraint('deployment_number', name='uq_deployment_number'),
    )

    op.create_table('duckdb_files',
        sa.Column('id', postgresql.UUID(as_uuid=False), primary_key=True),
        sa.Column('s3_path', sa.Text, unique=True, nullable=False),
        sa.Column('pipeline_id', sa.String(200), nullable=False),
        sa.Column('run_id', sa.String(500), nullable=False),
        sa.Column('task_id', sa.String(200), nullable=False),
        sa.Column('table_name', sa.String(200)),
        sa.Column('schema', postgresql.JSONB),
        sa.Column('row_count', sa.BigInteger),
        sa.Column('size_bytes', sa.BigInteger),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('expires_at', sa.DateTime(timezone=True)),
        sa.Column('is_intermediate', sa.Boolean, default=True),
        sa.Column('metadata', postgresql.JSONB),
    )

    op.create_table('eks_jobs',
        sa.Column('id', postgresql.UUID(as_uuid=False), primary_key=True),
        sa.Column('job_name', sa.String(500), unique=True, nullable=False),
        sa.Column('task_run_id', sa.String(500), sa.ForeignKey('task_runs.task_run_id')),
        sa.Column('pipeline_id', sa.String(200), nullable=False),
        sa.Column('run_id', sa.String(500), nullable=False),
        sa.Column('task_id', sa.String(200), nullable=False),
        sa.Column('image', sa.String(500), nullable=False),
        sa.Column('namespace', sa.String(200), default='nextgen-databridge-jobs'),
        sa.Column('cpu_request', sa.String(20)),
        sa.Column('memory_request', sa.String(20)),
        sa.Column('cpu_limit', sa.String(20)),
        sa.Column('memory_limit', sa.String(20)),
        sa.Column('status', sa.String(50), default='pending'),
        sa.Column('pod_name', sa.String(500)),
        sa.Column('node_name', sa.String(500)),
        sa.Column('exit_code', sa.Integer),
        sa.Column('submitted_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('started_at', sa.DateTime(timezone=True)),
        sa.Column('completed_at', sa.DateTime(timezone=True)),
        sa.Column('env_vars', postgresql.JSONB),
        sa.Column('manifest', postgresql.JSONB),
    )

    op.create_table('pipeline_metrics',
        sa.Column('id', postgresql.UUID(as_uuid=False), primary_key=True),
        sa.Column('pipeline_id', sa.String(200), nullable=False),
        sa.Column('metric_date', sa.DateTime(timezone=True), nullable=False),
        sa.Column('total_runs', sa.Integer, default=0),
        sa.Column('successful_runs', sa.Integer, default=0),
        sa.Column('failed_runs', sa.Integer, default=0),
        sa.Column('avg_duration_seconds', sa.Float),
        sa.Column('p95_duration_seconds', sa.Float),
        sa.Column('total_rows_processed', sa.BigInteger, default=0),
        sa.Column('sla_breaches', sa.Integer, default=0),
        sa.UniqueConstraint('pipeline_id', 'metric_date', name='uq_pipeline_metrics_date'),
    )


def downgrade():
    for tbl in ['pipeline_metrics','eks_jobs','duckdb_files','deployments','alert_rules',
                'alerts','data_connections','audit_logs','task_runs','pipeline_runs',
                'pipeline_configs','pipelines']:
        op.drop_table(tbl)
    for enum in ['pipelinestatus','runstatus','taskstatus','tasktype','connectiontype',
                 'deploymentstatus','deploymentenvironment','alertseverity','auditeventtype']:
        op.execute(f'DROP TYPE IF EXISTS {enum}')
