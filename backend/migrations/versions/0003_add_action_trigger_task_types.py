"""Add api_call, autosys_job, stored_proc to tasktype enum

Revision ID: 0003_add_action_trigger_types
Revises: 0002_fix_enum_case
Create Date: 2026-04-30
"""
from alembic import op
from sqlalchemy import text

revision      = '0003_add_action_trigger_types'
down_revision = '0002_fix_enum_case'
branch_labels = None
depends_on    = None


def upgrade():
    conn = op.get_bind()
    conn.execute(text(
        "ALTER TABLE task_runs ALTER COLUMN task_type TYPE TEXT USING task_type::TEXT"
    ))
    conn.execute(text("DROP TYPE IF EXISTS tasktype CASCADE"))
    conn.execute(text(
        "CREATE TYPE tasktype AS ENUM ("
        "'sql_extract','sql_transform','duckdb_transform','duckdb_query',"
        "'schema_validate','data_quality','cdc_extract','file_ingest',"
        "'kafka_consume','kafka_produce','pubsub_consume','pubsub_publish',"
        "'eks_job','eks_extract','eks_transform',"
        "'python_callable','conditional_branch','load_target','notification',"
        "'api_call','autosys_job','stored_proc')"
    ))
    conn.execute(text(
        "ALTER TABLE task_runs ALTER COLUMN task_type TYPE tasktype "
        "USING task_type::tasktype"
    ))


def downgrade():
    pass
