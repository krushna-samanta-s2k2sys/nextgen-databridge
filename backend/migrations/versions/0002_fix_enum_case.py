"""Fix enum case: convert PostgreSQL enum labels from NAME-case (ACTIVE) to value-case (active)

SQLAlchemy's create_all() with Python Enum types uses enum NAMES as PostgreSQL labels
by default, but the code reads/writes enum VALUES. This migration converts all uppercase
enum labels in the DB to the lowercase values the application expects.

Revision ID: 0002_fix_enum_case
Revises: 0001_initial
Create Date: 2026-04-29
"""
from alembic import op
from sqlalchemy import text

revision      = '0002_fix_enum_case'
down_revision = '0001_initial'
branch_labels = None
depends_on    = None


def _convert_enum(conn, table: str, column: str, old_type: str, new_type: str, values: list[str]):
    """
    Convert an enum column from (possibly uppercase) labels to lowercase values.
    Steps: column → TEXT (LOWER) → drop old type → create new type → column → enum
    """
    conn.execute(text(
        f"ALTER TABLE {table} ALTER COLUMN {column} TYPE TEXT "
        f"USING LOWER({column}::TEXT)"
    ))
    conn.execute(text(f"DROP TYPE IF EXISTS {old_type} CASCADE"))
    labels = ", ".join(f"'{v}'" for v in values)
    conn.execute(text(f"CREATE TYPE {new_type} AS ENUM ({labels})"))
    conn.execute(text(
        f"ALTER TABLE {table} ALTER COLUMN {column} TYPE {new_type} "
        f"USING {column}::{new_type}"
    ))


def upgrade():
    conn = op.get_bind()

    # ── pipelinestatus ────────────────────────────────────────────────────────
    conn.execute(text("ALTER TABLE pipelines ALTER COLUMN status TYPE TEXT USING LOWER(status::TEXT)"))
    conn.execute(text("DROP TYPE IF EXISTS pipelinestatus CASCADE"))
    conn.execute(text("CREATE TYPE pipelinestatus AS ENUM ('active','paused','deprecated','draft')"))
    conn.execute(text("ALTER TABLE pipelines ALTER COLUMN status TYPE pipelinestatus USING status::pipelinestatus"))
    conn.execute(text("ALTER TABLE pipelines ALTER COLUMN status SET DEFAULT 'active'"))

    # ── runstatus ─────────────────────────────────────────────────────────────
    conn.execute(text("ALTER TABLE pipeline_runs ALTER COLUMN status TYPE TEXT USING LOWER(status::TEXT)"))
    conn.execute(text("DROP TYPE IF EXISTS runstatus CASCADE"))
    conn.execute(text("CREATE TYPE runstatus AS ENUM ('running','success','failed','queued','cancelled','skipped')"))
    conn.execute(text("ALTER TABLE pipeline_runs ALTER COLUMN status TYPE runstatus USING status::runstatus"))
    conn.execute(text("ALTER TABLE pipeline_runs ALTER COLUMN status SET DEFAULT 'queued'"))

    # ── taskstatus ────────────────────────────────────────────────────────────
    conn.execute(text("ALTER TABLE task_runs ALTER COLUMN status TYPE TEXT USING LOWER(status::TEXT)"))
    conn.execute(text("DROP TYPE IF EXISTS taskstatus CASCADE"))
    conn.execute(text(
        "CREATE TYPE taskstatus AS ENUM "
        "('pending','running','success','failed','skipped','upstream_failed','retrying')"
    ))
    conn.execute(text("ALTER TABLE task_runs ALTER COLUMN status TYPE taskstatus USING status::taskstatus"))
    conn.execute(text("ALTER TABLE task_runs ALTER COLUMN status SET DEFAULT 'pending'"))

    # ── tasktype ──────────────────────────────────────────────────────────────
    # Also add eks_extract and eks_transform that EKS jobs write directly via SQL
    conn.execute(text("ALTER TABLE task_runs ALTER COLUMN task_type TYPE TEXT USING LOWER(task_type::TEXT)"))
    conn.execute(text("DROP TYPE IF EXISTS tasktype CASCADE"))
    conn.execute(text(
        "CREATE TYPE tasktype AS ENUM ("
        "'sql_extract','sql_transform','duckdb_transform','duckdb_query',"
        "'schema_validate','data_quality','cdc_extract','file_ingest',"
        "'kafka_consume','kafka_produce','pubsub_consume','pubsub_publish',"
        "'eks_job','eks_extract','eks_transform',"
        "'python_callable','conditional_branch','load_target','notification')"
    ))
    conn.execute(text("ALTER TABLE task_runs ALTER COLUMN task_type TYPE tasktype USING task_type::tasktype"))

    # ── connectiontype ────────────────────────────────────────────────────────
    conn.execute(text("ALTER TABLE data_connections ALTER COLUMN connection_type TYPE TEXT USING LOWER(connection_type::TEXT)"))
    conn.execute(text("DROP TYPE IF EXISTS connectiontype CASCADE"))
    conn.execute(text(
        "CREATE TYPE connectiontype AS ENUM "
        "('sqlserver','oracle','postgresql','mysql','kafka','pubsub','s3','gcs','file','http','redis')"
    ))
    conn.execute(text("ALTER TABLE data_connections ALTER COLUMN connection_type TYPE connectiontype USING connection_type::connectiontype"))

    # ── deploymentstatus ──────────────────────────────────────────────────────
    conn.execute(text("ALTER TABLE deployments ALTER COLUMN status TYPE TEXT USING LOWER(status::TEXT)"))
    conn.execute(text("DROP TYPE IF EXISTS deploymentstatus CASCADE"))
    conn.execute(text(
        "CREATE TYPE deploymentstatus AS ENUM "
        "('draft','pending_approval','approved','rejected','deploying','deployed','failed','rolled_back')"
    ))
    conn.execute(text("ALTER TABLE deployments ALTER COLUMN status TYPE deploymentstatus USING status::deploymentstatus"))
    conn.execute(text("ALTER TABLE deployments ALTER COLUMN status SET DEFAULT 'draft'"))

    # ── deploymentenvironment ─────────────────────────────────────────────────
    conn.execute(text("ALTER TABLE deployments ALTER COLUMN environment TYPE TEXT USING LOWER(environment::TEXT)"))
    conn.execute(text("DROP TYPE IF EXISTS deploymentenvironment CASCADE"))
    conn.execute(text("CREATE TYPE deploymentenvironment AS ENUM ('dev','staging','production')"))
    conn.execute(text("ALTER TABLE deployments ALTER COLUMN environment TYPE deploymentenvironment USING environment::deploymentenvironment"))

    # ── alertseverity (used in alerts, alert_rules, audit_logs) ──────────────
    conn.execute(text("ALTER TABLE alerts ALTER COLUMN severity TYPE TEXT USING LOWER(severity::TEXT)"))
    conn.execute(text("ALTER TABLE alert_rules ALTER COLUMN severity TYPE TEXT USING LOWER(severity::TEXT)"))
    conn.execute(text("ALTER TABLE audit_logs ALTER COLUMN severity TYPE TEXT USING LOWER(severity::TEXT)"))
    conn.execute(text("DROP TYPE IF EXISTS alertseverity CASCADE"))
    conn.execute(text("CREATE TYPE alertseverity AS ENUM ('info','warning','error','critical')"))
    conn.execute(text("ALTER TABLE alerts ALTER COLUMN severity TYPE alertseverity USING severity::alertseverity"))
    conn.execute(text("ALTER TABLE alert_rules ALTER COLUMN severity TYPE alertseverity USING severity::alertseverity"))
    conn.execute(text("ALTER TABLE audit_logs ALTER COLUMN severity TYPE alertseverity USING severity::alertseverity"))
    conn.execute(text("ALTER TABLE alerts ALTER COLUMN severity SET DEFAULT 'error'"))
    conn.execute(text("ALTER TABLE alert_rules ALTER COLUMN severity SET DEFAULT 'error'"))
    conn.execute(text("ALTER TABLE audit_logs ALTER COLUMN severity SET DEFAULT 'info'"))

    # ── auditeventtype: values are already uppercase (names == values) ─────────
    # No conversion needed — Python enum values are 'PIPELINE_CREATED' etc. (uppercase)
    # Only ensure the type exists with correct labels if it was somehow corrupted
    # (create_all uses names which equal values here, so no mismatch)


def downgrade():
    # No rollback — converting back to uppercase would be destructive
    pass
