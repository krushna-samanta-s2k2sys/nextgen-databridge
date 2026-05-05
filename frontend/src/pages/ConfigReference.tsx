import React, { useState, useMemo } from 'react'
import Editor from '@monaco-editor/react'
import {
  BookOpen, Search, ChevronRight, Copy, CheckCircle2,
  Database, Cpu, FileText, Bell, Zap, GitBranch,
  ArrowRight, Table2, MessageSquare, Cloud, Webhook,
  Calendar, Server, Network,
} from 'lucide-react'
import clsx from 'clsx'

// ── Types ─────────────────────────────────────────────────────────────────────
interface FieldDef {
  field: string
  type: string
  required: boolean
  default?: string
  valid?: string
  description: string
}

interface TaskDef {
  id: string
  label: string
  category: string
  icon: React.ElementType
  color: string
  description: string
  sample: string
  fields: FieldDef[]
}

// ── Categories ────────────────────────────────────────────────────────────────
const CATEGORIES = [
  { id: 'pipeline',    label: 'Pipeline Envelope',   icon: FileText    },
  { id: 'dependencies', label: 'DAG Dependencies',   icon: Network     },
  { id: 'connection',  label: 'Connection Profiles',  icon: Database    },
  { id: 'extract',     label: 'Data Extraction',      icon: ArrowRight  },
  { id: 'transform',   label: 'Transformation',       icon: Cpu         },
  { id: 'quality',     label: 'Data Quality',         icon: CheckCircle2},
  { id: 'file',        label: 'File & Messaging',     icon: MessageSquare},
  { id: 'compute',     label: 'Compute (EKS)',        icon: Server      },
  { id: 'load',        label: 'Load & Notify',        icon: Cloud       },
  { id: 'triggers',    label: 'Action Triggers',      icon: Webhook     },
  { id: 'control',     label: 'Control Flow',         icon: GitBranch   },
]

// ── Reference data ────────────────────────────────────────────────────────────
const TASK_DEFS: TaskDef[] = [

  // ── Pipeline Envelope ────────────────────────────────────────────────────
  {
    id: 'pipeline_envelope',
    label: 'Pipeline Config',
    category: 'pipeline',
    icon: FileText,
    color: 'text-slate-600',
    description: 'Top-level pipeline definition. Every pipeline config must have a pipeline_id and tasks array. All other fields are optional but recommended.',
    fields: [
      { field: 'pipeline_id', type: 'string', required: true,  description: 'Unique snake_case identifier. Must match [a-z][a-z0-9_]{1,198}[a-z0-9].' },
      { field: 'version',     type: 'string', required: false, default: '—',       description: 'Semver string (e.g. "1.0.0"). Used for config history tracking.' },
      { field: 'description', type: 'string', required: false, description: 'Human-readable description shown in the UI.' },
      { field: 'schedule',    type: 'string', required: false, default: 'null',    description: 'Standard 5-field cron expression (e.g. "0 6 * * *"). Omit for manual-only.' },
      { field: 'retries',     type: 'integer',required: false, default: '3',       valid: '0–10', description: 'Default retry count applied to every task (can be overridden per task).' },
      { field: 'retry_delay_minutes', type: 'integer', required: false, default: '5', description: 'Minutes between retries.' },
      { field: 'retry_exponential_backoff', type: 'boolean', required: false, default: 'false', description: 'Double the retry delay on each attempt.' },
      { field: 'sla_minutes', type: 'number', required: false, description: 'Trigger an SLA breach alert if the run exceeds this duration.' },
      { field: 'max_active_runs', type: 'integer', required: false, default: '1', description: 'Maximum concurrent DAG runs.' },
      { field: 'catchup',     type: 'boolean',required: false, default: 'false',   description: 'Whether Airflow should backfill missed scheduled runs.' },
      { field: 'dag_timeout_minutes', type: 'integer', required: false, default: '120', description: 'Hard timeout for the entire DAG run.' },
      { field: 'owner',       type: 'string', required: false, default: '"data-engineering"', description: 'Team or individual responsible for this pipeline.' },
      { field: 'tags',        type: 'string[]',required: false,description: 'Free-form labels shown in Airflow and the UI.' },
      { field: 'alerting',    type: 'object', required: false, description: 'Alert channels per event. Keys: on_failure, on_success, on_sla_breach, on_retry. Values: list of channel strings (slack:#ch, email:addr, pagerduty:key, teams:webhook, webhook:url).' },
      { field: 'dag_dependencies', type: 'object', required: false, description: 'Cross-pipeline dependencies. upstream[] defines DAGs that must complete before this one starts; downstream[] defines DAGs this pipeline triggers on completion. See the DAG Dependencies reference for full schema.' },
      { field: 'tasks',       type: 'Task[]', required: true,  description: 'Ordered list of task definitions. Dependency order is declared via depends_on inside each task, not by array position.' },
    ],
    sample: `{
  "pipeline_id": "wwi_sales_daily",
  "version": "1.0.0",
  "description": "Daily sales ETL from WideWorldImporters SQL Server to target DB.",
  "schedule": "0 6 * * *",
  "retries": 3,
  "retry_delay_minutes": 5,
  "retry_exponential_backoff": true,
  "sla_minutes": 60,
  "max_active_runs": 1,
  "catchup": false,
  "dag_timeout_minutes": 120,
  "owner": "data-engineering",
  "team": "analytics",
  "tags": ["wwi", "sales", "daily"],
  "alerting": {
    "on_failure":    ["slack:#data-alerts", "email:data-eng@company.com"],
    "on_sla_breach": ["slack:#data-alerts"],
    "on_success":    ["slack:#data-notifications"]
  },
  "dag_dependencies": {
    "upstream":   [ /* see DAG Dependencies reference */ ],
    "downstream": [ /* see DAG Dependencies reference */ ]
  },
  "tasks": [ /* task definitions go here */ ]
}`,
  },

  // ── DAG Dependencies ────────────────────────────────────────────────────
  {
    id: 'dag_dependencies',
    label: 'DAG Dependencies',
    category: 'dependencies',
    icon: Network,
    color: 'text-violet-600',
    description: 'Cross-pipeline dependency management. upstream[] defines DAGs that must complete before this pipeline starts; downstream[] defines DAGs this pipeline triggers on completion. Each entry needs only a name and the status to await.',
    fields: [
      { field: 'dag_dependencies',           type: 'object',   required: false, description: 'Top-level container. Place at the pipeline root level alongside "tasks".' },
      { field: 'dag_dependencies.upstream',  type: 'object[]', required: false, description: 'DAGs that must finish before this pipeline\'s first task runs. Creates one ExternalTaskSensor per entry.' },
      { field: '  upstream[].pipeline_id',   type: 'string',   required: true,  description: 'pipeline_id of the upstream DAG to wait for.' },
      { field: '  upstream[].await_status',  type: 'string',   required: false, default: '"success"', valid: '"success" | "failed" | "skipped" | "upstream_failed"', description: 'The DAG run state that unblocks this pipeline.' },
      { field: 'dag_dependencies.downstream',type: 'object[]', required: false, description: 'DAGs to trigger after all tasks in this pipeline succeed. Creates one TriggerDagRunOperator per entry and waits for it to complete.' },
      { field: '  downstream[].pipeline_id', type: 'string',   required: true,  description: 'pipeline_id of the downstream DAG to trigger.' },
    ],
    sample: `{
  "pipeline_id": "wwi_analytics_summary",
  "schedule": "0 8 * * *",

  "dag_dependencies": {
    "upstream": [
      { "pipeline_id": "wwi_sales_etl",     "await_status": "success" },
      { "pipeline_id": "wwi_inventory_etl", "await_status": "success" }
    ],
    "downstream": [
      { "pipeline_id": "wwi_reporting_refresh" }
    ]
  },

  "tasks": [ /* ... */ ]
}`,
  },

  // ── Connection Profiles ──────────────────────────────────────────────────
  {
    id: 'connection_oracle',
    label: 'Oracle Connection',
    category: 'connection',
    icon: Database,
    color: 'text-red-600',
    description: 'Oracle Database connection profile stored in configs/environments/<env>.json. Credentials are never stored in the file — they reference an AWS Secrets Manager secret. The driver field controls whether python-oracledb (thin, no Instant Client) or cx_Oracle (thick) is used.',
    fields: [
      { field: 'airflow_conn_id', type: 'string', required: true,  description: 'Airflow connection ID referenced by task configs. Must be unique per environment.' },
      { field: 'type',           type: 'string', required: true,  valid: '"oracle"', description: 'Connection type — must be "oracle".' },
      { field: 'host',           type: 'string', required: true,  description: 'Hostname or IP of the Oracle database server.' },
      { field: 'port',           type: 'integer',required: false, default: '1521',  description: 'Oracle listener port.' },
      { field: 'service_name',   type: 'string', required: true,  description: 'Oracle service name (preferred over SID). Used in the DSN: host:port/service_name.' },
      { field: 'schema',         type: 'string', required: false, description: 'Default schema/user to connect as. Queries run under this context.' },
      { field: 'driver',         type: 'string', required: false, default: '"oracledb"', valid: '"oracledb" | "cx_Oracle"', description: 'oracledb = python-oracledb thin mode (no Oracle Instant Client required). cx_Oracle = thick mode (requires Oracle Instant Client installed).' },
      { field: 'secret_name',    type: 'string', required: true,  description: 'AWS Secrets Manager secret path. Secret must contain JSON with keys: username, password.' },
      { field: 'notes',          type: 'string', required: false, description: 'Free-text notes for documentation purposes only.' },
    ],
    sample: `// In configs/environments/dev.json → "connections" block
{
  "domedit_oracle": {
    "airflow_conn_id": "domedit_oracle",
    "type": "oracle",
    "host": "domedit-db.corp.internal",
    "port": 1521,
    "service_name": "DOMEDIT",
    "schema": "SCHEMA1",
    "driver": "oracledb",
    "secret_name": "nextgen-databridge/connections/domedit_oracle",
    "notes": "DOMEDIT Oracle DB — thin-mode oracledb (no Instant Client required)."
  }
}

// Secrets Manager secret  nextgen-databridge/connections/domedit_oracle
// must contain:
// { "username": "svc_databridge", "password": "<vault-managed>" }`,
  },
  {
    id: 'connection_mssql',
    label: 'SQL Server Connection',
    category: 'connection',
    icon: Database,
    color: 'text-sky-600',
    description: 'Microsoft SQL Server connection profile. Uses pyodbc (ODBC Driver 18) or pymssql as fallback.',
    fields: [
      { field: 'airflow_conn_id', type: 'string', required: true,  description: 'Airflow connection ID referenced in task source.connection.' },
      { field: 'type',           type: 'string', required: true,  valid: '"mssql"', description: 'Must be "mssql".' },
      { field: 'port',           type: 'integer',required: false, default: '1433',  description: 'SQL Server port.' },
      { field: 'database',       type: 'string', required: true,  description: 'Database / catalog name.' },
      { field: 'secret_name',    type: 'string', required: true,  description: 'Secrets Manager secret path. Must contain: host, username, password.' },
    ],
    sample: `{
  "wwi_sqlserver": {
    "airflow_conn_id": "wwi_sqlserver",
    "type": "mssql",
    "port": 1433,
    "database": "WideWorldImporters",
    "secret_name": "nextgen-databridge/connections/wwi_sqlserver"
  }
}`,
  },
  {
    id: 'connection_postgresql',
    label: 'PostgreSQL Connection',
    category: 'connection',
    icon: Database,
    color: 'text-indigo-600',
    description: 'PostgreSQL connection profile. Uses psycopg2.',
    fields: [
      { field: 'airflow_conn_id', type: 'string', required: true,  description: 'Airflow connection ID.' },
      { field: 'type',           type: 'string', required: true,  valid: '"postgresql"', description: 'Must be "postgresql".' },
      { field: 'port',           type: 'integer',required: false, default: '5432',  description: 'PostgreSQL port.' },
      { field: 'database',       type: 'string', required: true,  description: 'Database name.' },
      { field: 'secret_name',    type: 'string', required: true,  description: 'Secrets Manager secret path. Must contain: host, username, password.' },
    ],
    sample: `{
  "audit_db": {
    "airflow_conn_id": "audit_db",
    "type": "postgresql",
    "port": 5432,
    "database": "airflow",
    "secret_name": "nextgen-databridge/connections/audit_db"
  }
}`,
  },

  // ── SQL Extract ──────────────────────────────────────────────────────────
  {
    id: 'sql_extract',
    label: 'sql_extract',
    category: 'extract',
    icon: ArrowRight,
    color: 'text-blue-600',
    description: 'Extracts rows from SQL Server, Oracle, or PostgreSQL into a DuckDB file on S3. Supports full-load and incremental patterns. Oracle uses python-oracledb thin mode.',
    fields: [
      { field: 'task_id',        type: 'string',  required: true,  description: 'Unique snake_case identifier within the pipeline.' },
      { field: 'type',           type: 'string',  required: true,  valid: '"sql_extract" | "cdc_extract"', description: 'Use "cdc_extract" when the query captures only changed rows (same operator, semantic hint).' },
      { field: 'depends_on',     type: 'string[]',required: false, description: 'List of task_ids this task waits for before starting.' },
      { field: 'source.connection', type: 'string', required: true,  description: 'Airflow connection ID (maps to airflow_conn_id in the environment config).' },
      { field: 'source.query',   type: 'string',  required: false, description: 'Full SQL SELECT. Supports {{ ds }} (run date) and {{ run_id }} template variables. Mutually exclusive with source.table.' },
      { field: 'source.table',   type: 'string',  required: false, description: 'Table name for "SELECT * FROM table" shortcut. Mutually exclusive with source.query.' },
      { field: 'source.batch_size', type: 'integer', required: false, default: '100000', description: 'Pandas chunksize for memory-efficient fetching of large result sets.' },
      { field: 'output.duckdb_path', type: 'string', required: false, description: 'S3 path for the output DuckDB file. Omit to use the auto-generated canonical path s3://{duckdb_store}/pipelines/{pipeline_id}/runs/{{ run_id }}/{task_id}.duckdb.' },
      { field: 'output.table',   type: 'string',  required: false, default: '"raw_data"', description: 'Table name inside the DuckDB file.' },
      { field: 'retries',        type: 'integer', required: false, description: 'Override the pipeline-level retry count for this task.' },
      { field: 'engine',         type: 'string',  required: false, valid: '"eks"', description: 'Set to "eks" to run this task as a Kubernetes Job for large extractions.' },
      { field: 'execution.memory', type: 'string', required: false, description: 'EKS pod memory limit, e.g. "8Gi". Only relevant when engine=eks.' },
      { field: 'execution.cpu',  type: 'string',  required: false, description: 'EKS pod CPU request, e.g. "2". Only relevant when engine=eks.' },
    ],
    sample: `{
  "task_id": "extract_domedit_orders",
  "type": "sql_extract",
  "source": {
    "connection": "domedit_oracle",
    "query": "SELECT order_id, customer_id, order_date, total_amount, status FROM SCHEMA1.ORDERS WHERE order_date >= TRUNC(SYSDATE) - 1 AND order_date < TRUNC(SYSDATE)",
    "batch_size": 50000
  },
  "output": {
    "duckdb_path": "s3://\${duckdb_store}/pipelines/domedit_etl/runs/{{ run_id }}/extract_domedit_orders.duckdb",
    "table": "raw_orders"
  }
}`,
  },
  {
    id: 'cdc_extract',
    label: 'cdc_extract',
    category: 'extract',
    icon: ArrowRight,
    color: 'text-cyan-600',
    description: 'CDC (Change Data Capture) extraction. Uses the same SQLExtractOperator as sql_extract but signals downstream tasks that only changed rows are present. Write your query to filter by a watermark column or CDC system table.',
    fields: [
      { field: 'type',           type: 'string',  required: true,  valid: '"cdc_extract"', description: 'Must be "cdc_extract".' },
      { field: 'source.connection', type: 'string', required: true,  description: 'Airflow connection ID.' },
      { field: 'source.query',   type: 'string',  required: true,  description: 'CDC query. Typically reads from a change table or filters by a watermark column using {{ ds }}.' },
      { field: 'source.batch_size', type: 'integer', required: false, default: '100000', description: 'Pandas chunksize.' },
      { field: 'output.duckdb_path', type: 'string', required: false, description: 'S3 path for output DuckDB.' },
      { field: 'output.table',   type: 'string',  required: false, default: '"cdc_changes"', description: 'Table name inside the DuckDB file.' },
    ],
    sample: `{
  "task_id": "cdc_orders_changes",
  "type": "cdc_extract",
  "source": {
    "connection": "wwi_sqlserver",
    "query": "SELECT ct.*, o.CustomerID, o.OrderDate FROM cdc.fn_cdc_get_all_changes_Sales_Orders(sys.fn_cdc_get_min_lsn('Sales_Orders'), sys.fn_cdc_get_max_lsn(), 'all') ct JOIN Sales.Orders o ON ct.OrderID = o.OrderID WHERE ct.__$operation IN (2, 4)",
    "batch_size": 100000
  },
  "output": {
    "table": "cdc_changes"
  }
}`,
  },

  // ── Transform ────────────────────────────────────────────────────────────
  {
    id: 'duckdb_transform',
    label: 'duckdb_transform',
    category: 'transform',
    icon: Cpu,
    color: 'text-violet-600',
    description: 'Runs SQL inside DuckDB, attaching upstream DuckDB files as read-only databases. Ideal for joins, aggregations, enrichment, and type casting across large datasets — all in-process without a separate database server.',
    fields: [
      { field: 'type',           type: 'string',  required: true,  valid: '"duckdb_transform" | "sql_transform" | "duckdb_query"', description: 'All three map to the same DuckDBTransformOperator.' },
      { field: 'depends_on',     type: 'string[]',required: true,  description: 'Upstream tasks whose DuckDB outputs are attached. Each upstream task_id becomes an ATTACH alias (underscores replace hyphens/slashes).' },
      { field: 'sql',            type: 'string',  required: true,  description: 'DuckDB SQL SELECT. Reference upstream tables as <task_id>.<table_name>. Supports {{ ds }} and {{ run_id }}.' },
      { field: 'source.duckdb_path', type: 'string', required: false, description: 'Explicitly attach an additional S3 DuckDB path not from depends_on.' },
      { field: 'output.duckdb_path', type: 'string', required: false, description: 'Output S3 path. Auto-generated if omitted.' },
      { field: 'output.table',   type: 'string',  required: false, default: '"result"', description: 'Table name in the output DuckDB file.' },
    ],
    sample: `{
  "task_id": "transform_enriched_orders",
  "type": "duckdb_transform",
  "depends_on": ["extract_domedit_orders", "extract_customers"],
  "sql": "SELECT o.order_id, o.order_date, o.total_amount, c.customer_name, c.region, CASE WHEN o.total_amount > 10000 THEN 'high' WHEN o.total_amount > 1000 THEN 'medium' ELSE 'low' END AS order_tier FROM extract_domedit_orders.raw_orders o LEFT JOIN extract_customers.raw_customers c ON o.customer_id = c.customer_id WHERE o.status != 'CANCELLED'",
  "output": {
    "table": "enriched_orders"
  }
}`,
  },

  // ── Data Quality ─────────────────────────────────────────────────────────
  {
    id: 'data_quality',
    label: 'data_quality',
    category: 'quality',
    icon: CheckCircle2,
    color: 'text-emerald-600',
    description: 'Runs configurable QC checks against a DuckDB table. Does NOT produce a new DuckDB file — it passes the upstream path through via XCom so downstream transforms can still attach it. On failure the pipeline is halted (configurable).',
    fields: [
      { field: 'type',           type: 'string',  required: true,  valid: '"data_quality"', description: 'Must be "data_quality".' },
      { field: 'depends_on',     type: 'string[]',required: true,  description: 'Single upstream task whose DuckDB output is checked.' },
      { field: 'source.table',   type: 'string',  required: false, default: '"raw_data"', description: 'Table name inside the upstream DuckDB file.' },
      { field: 'checks',         type: 'Check[]', required: true,  description: 'List of check objects (see check types below).' },
      { field: 'checks[].type',  type: 'string',  required: true,  valid: '"not_null" | "unique" | "row_count_min" | "row_count_max" | "freshness" | "value_range" | "regex_match" | "schema_match" | "custom_sql"', description: 'Type of quality check to run.' },
      { field: 'checks[].column',type: 'string',  required: false, description: 'Column to check. Required for not_null, unique, value_range, regex_match, freshness.' },
      { field: 'checks[].action',type: 'string',  required: false, default: '"fail"', valid: '"fail" | "warn" | "skip"', description: 'What to do when the check fails. "warn" records the failure but does not stop the pipeline.' },
      { field: 'checks[].value', type: 'number',  required: false, description: 'Threshold value for row_count_min / row_count_max checks.' },
      { field: 'checks[].min / max', type: 'number', required: false, description: 'Range boundaries for value_range checks.' },
      { field: 'checks[].pattern', type: 'string', required: false, description: 'Regex pattern for regex_match checks.' },
      { field: 'checks[].max_hours', type: 'number', required: false, description: 'Maximum staleness in hours for freshness checks.' },
      { field: 'checks[].sql',   type: 'string',  required: false, description: 'Custom SQL for custom_sql checks. Must return a count of failing rows (0 = pass).' },
      { field: 'fail_pipeline_on_error', type: 'boolean', required: false, default: 'true', description: 'Set to false to record failures without halting the pipeline.' },
    ],
    sample: `{
  "task_id": "validate_orders",
  "type": "data_quality",
  "depends_on": ["extract_domedit_orders"],
  "source": { "table": "raw_orders" },
  "checks": [
    { "type": "not_null",      "column": "order_id",     "action": "fail" },
    { "type": "not_null",      "column": "order_date",   "action": "fail" },
    { "type": "unique",        "column": "order_id",     "action": "fail" },
    { "type": "row_count_min", "value": 1,               "action": "warn" },
    { "type": "value_range",   "column": "total_amount", "min": 0, "action": "fail" },
    { "type": "freshness",     "column": "order_date",   "max_hours": 26, "action": "warn" },
    { "type": "custom_sql",
      "sql": "SELECT COUNT(*) FROM raw_orders WHERE status NOT IN ('OPEN','CLOSED','CANCELLED')",
      "action": "fail" }
  ],
  "fail_pipeline_on_error": true
}`,
  },
  {
    id: 'schema_validate',
    label: 'schema_validate',
    category: 'quality',
    icon: Table2,
    color: 'text-teal-600',
    description: 'Compares the actual schema of a DuckDB table against an expected schema. Fails the task if columns are missing or have wrong types. Passes the DuckDB path through so downstream tasks can still attach it.',
    fields: [
      { field: 'type',           type: 'string',  required: true,  valid: '"schema_validate"', description: 'Must be "schema_validate".' },
      { field: 'depends_on',     type: 'string[]',required: true,  description: 'Upstream task whose DuckDB schema is checked.' },
      { field: 'source.table',   type: 'string',  required: false, default: '"raw_data"', description: 'Table name to inspect inside the DuckDB file.' },
      { field: 'expected_schema', type: 'object', required: true,  description: 'Map of column_name → DuckDB type string. Type matching is case-insensitive substring (e.g. "INTEGER" matches "INTEGER", "INT32", "HUGEINT").' },
      { field: 'fail_on_error',  type: 'boolean', required: false, default: 'true', description: 'Whether to raise on schema mismatch.' },
    ],
    sample: `{
  "task_id": "schema_check_orders",
  "type": "schema_validate",
  "depends_on": ["extract_domedit_orders"],
  "source": { "table": "raw_orders" },
  "expected_schema": {
    "order_id":     "INTEGER",
    "customer_id":  "INTEGER",
    "order_date":   "DATE",
    "total_amount": "DOUBLE",
    "status":       "VARCHAR"
  },
  "fail_on_error": true
}`,
  },

  // ── File & Messaging ─────────────────────────────────────────────────────
  {
    id: 'file_ingest',
    label: 'file_ingest',
    category: 'file',
    icon: FileText,
    color: 'text-amber-600',
    description: 'Reads a file from local disk or S3 (CSV, TSV, Parquet, JSON/JSONL, Excel) and loads it into a DuckDB table on S3.',
    fields: [
      { field: 'type',           type: 'string',  required: true,  valid: '"file_ingest"', description: 'Must be "file_ingest".' },
      { field: 'source.path',    type: 'string',  required: true,  description: 'Local path or s3://bucket/key. S3 files are downloaded to /tmp before loading.' },
      { field: 'source.format',  type: 'string',  required: false, default: '"csv"', valid: '"csv" | "tsv" | "parquet" | "json" | "jsonl" | "ndjson" | "xlsx" | "xls"', description: 'File format.' },
      { field: 'source.delimiter', type: 'string', required: false, default: '","', description: 'CSV column delimiter. Ignored for TSV (uses tab).' },
      { field: 'source.read_options', type: 'object', required: false, description: 'Extra kwargs passed to pandas read_csv / read_json, e.g. {"header": 0, "skiprows": 1}.' },
      { field: 'output.table',   type: 'string',  required: false, default: '"file_data"', description: 'Table name in the output DuckDB.' },
      { field: 'output.duckdb_path', type: 'string', required: false, description: 'Output S3 path. Auto-generated if omitted.' },
    ],
    sample: `{
  "task_id": "ingest_price_list",
  "type": "file_ingest",
  "source": {
    "path": "s3://nextgen-databridge-artifacts-dev/uploads/price_list_{{ ds }}.csv",
    "format": "csv",
    "delimiter": ",",
    "read_options": { "header": 0, "dtype": { "sku": "str" } }
  },
  "output": {
    "table": "price_list"
  }
}`,
  },
  {
    id: 'kafka_consume',
    label: 'kafka_consume',
    category: 'file',
    icon: MessageSquare,
    color: 'text-orange-600',
    description: 'Consumes messages from a Kafka topic into a DuckDB table. Commits offsets after processing. Messages must be JSON.',
    fields: [
      { field: 'type',           type: 'string',  required: true,  valid: '"kafka_consume" | "kafka_produce"', description: 'kafka_consume reads from a topic; kafka_produce (via LoadTargetOperator) writes to one.' },
      { field: 'connection',     type: 'string',  required: false, description: 'Airflow connection ID. Bootstrap servers fall back to KAFKA_BOOTSTRAP_SERVERS env var.' },
      { field: 'topic',          type: 'string',  required: true,  description: 'Kafka topic name.' },
      { field: 'group_id',       type: 'string',  required: false, default: '"nextgen-databridge-{pipeline_id}"', description: 'Consumer group ID.' },
      { field: 'max_messages',   type: 'integer', required: false, default: '10000', description: 'Stop consuming after this many messages.' },
      { field: 'poll_timeout_seconds', type: 'integer', required: false, default: '30', description: 'Seconds to wait for messages before stopping.' },
      { field: 'auto_offset_reset', type: 'string', required: false, default: '"earliest"', valid: '"earliest" | "latest"', description: 'Where to start reading if no committed offset exists.' },
      { field: 'output.table',   type: 'string',  required: false, default: '"kafka_messages"', description: 'Table name in the output DuckDB.' },
    ],
    sample: `{
  "task_id": "consume_order_events",
  "type": "kafka_consume",
  "topic": "orders.created.v1",
  "group_id": "nextgen-databridge-domedit-etl",
  "max_messages": 50000,
  "poll_timeout_seconds": 60,
  "auto_offset_reset": "earliest",
  "output": {
    "table": "kafka_orders"
  }
}`,
  },
  {
    id: 'pubsub',
    label: 'pubsub_consume',
    category: 'file',
    icon: Cloud,
    color: 'text-sky-600',
    description: 'Pulls messages from a GCP PubSub subscription into a DuckDB table. Acknowledges messages after processing.',
    fields: [
      { field: 'type',           type: 'string',  required: true,  valid: '"pubsub_consume" | "pubsub_publish"', description: 'pubsub_consume reads from a subscription; pubsub_publish writes (via LoadTargetOperator).' },
      { field: 'project_id',     type: 'string',  required: false, description: 'GCP project ID. Falls back to GCP_PROJECT_ID env var.' },
      { field: 'subscription',   type: 'string',  required: true,  description: 'PubSub subscription name.' },
      { field: 'topic',          type: 'string',  required: false, description: 'Topic name (informational / used for audit logging).' },
      { field: 'max_messages',   type: 'integer', required: false, default: '10000', description: 'Maximum messages to pull.' },
      { field: 'poll_timeout_seconds', type: 'integer', required: false, default: '60', description: 'Seconds to wait for messages.' },
      { field: 'output.table',   type: 'string',  required: false, default: '"pubsub_messages"', description: 'Table name in the output DuckDB.' },
    ],
    sample: `{
  "task_id": "consume_gcp_events",
  "type": "pubsub_consume",
  "project_id": "my-gcp-project",
  "subscription": "orders-subscription",
  "topic": "orders-topic",
  "max_messages": 10000,
  "poll_timeout_seconds": 30,
  "output": { "table": "pubsub_orders" }
}`,
  },

  // ── Compute ──────────────────────────────────────────────────────────────
  {
    id: 'eks_job',
    label: 'eks_job',
    category: 'compute',
    icon: Server,
    color: 'text-purple-600',
    description: 'Runs a task inside an EKS Kubernetes Job instead of the MWAA worker. Add "engine": "eks" to any task type (sql_extract, duckdb_transform, etc.) to route it to EKS — the DAG generator detects this and switches the operator automatically. The operation type (extract vs transform) is inferred from the original task type. Use "type": "eks_job" only for a standalone EKS task with a fully custom container. The default image is the platform transform ECR image; override with execution.image only when using a custom container.',
    fields: [
      { field: 'engine',         type: 'string',  required: false, valid: '"eks"', description: 'Add to any task type to route execution to EKS. The task type (sql_extract, duckdb_transform, etc.) is preserved for OPERATION_TYPE inference.' },
      { field: 'type',           type: 'string',  required: true,  valid: '"eks_job"', description: 'Use "eks_job" only for a standalone EKS task. For all other task types, keep their own type and add "engine": "eks" instead.' },
      { field: 'execution.image', type: 'string', required: false, description: 'ECR image URI. Defaults to the platform transform image for the current environment. Override only when using a custom container.' },
      { field: 'execution.cpu',  type: 'string',  required: false, default: '"1"',    description: 'CPU request in Kubernetes format, e.g. "250m", "2". Also used as cpu_limit if cpu_limit is not set.' },
      { field: 'execution.memory', type: 'string', required: false, default: '"2Gi"', description: 'Memory request, e.g. "512Mi", "4Gi". Also used as memory_limit if memory_limit is not set.' },
      { field: 'execution.cpu_limit',    type: 'string',  required: false, description: 'CPU limit. Defaults to execution.cpu.' },
      { field: 'execution.memory_limit', type: 'string',  required: false, description: 'Memory limit. Defaults to execution.memory.' },
      { field: 'execution.namespace', type: 'string', required: false, default: '"nextgen-databridge-jobs"', description: 'Kubernetes namespace for the Job.' },
      { field: 'execution.service_account', type: 'string', required: false, default: '"nextgen-databridge-task-runner"', description: 'K8s service account with AWS IAM IRSA annotations.' },
      { field: 'execution.env_vars', type: 'object', required: false, description: 'Extra environment variables injected into the pod. OPERATION_TYPE is always set automatically.' },
      { field: 'timeout_minutes', type: 'integer', required: false, default: '120',   description: 'Hard timeout — the task fails if the Job does not complete within this time.' },
      { field: 'depends_on',     type: 'string[]',required: false, description: 'Upstream tasks. Their DuckDB S3 paths are passed in INPUT_PATHS as a JSON map.' },
    ],
    sample: `{
  "task_id": "extract_large_dataset",
  "type": "sql_extract",
  "engine": "eks",
  "depends_on": [],
  "source": {
    "connection": "domedit_oracle",
    "query": "SELECT * FROM LARGE_TABLE WHERE PROCESS_DATE = :run_date"
  },
  "execution": {
    "cpu": "250m",
    "memory": "512Mi",
    "cpu_limit": "500m",
    "memory_limit": "1Gi"
  },
  "timeout_minutes": 30
}`,
  },

  // ── Load Target ──────────────────────────────────────────────────────────
  {
    id: 'load_target',
    label: 'load_target',
    category: 'load',
    icon: Cloud,
    color: 'text-green-600',
    description: 'Reads a DuckDB table from an upstream task and writes it to a target system: SQL Server, Oracle, PostgreSQL, S3/Parquet, Kafka, or GCP PubSub.',
    fields: [
      { field: 'type',           type: 'string',  required: true,  valid: '"load_target"', description: 'Must be "load_target".' },
      { field: 'depends_on',     type: 'string[]',required: true,  description: 'Single upstream task whose DuckDB output is read.' },
      { field: 'source.table',   type: 'string',  required: false, default: '"result"', description: 'Table name inside the upstream DuckDB file to read.' },
      { field: 'target.type',    type: 'string',  required: true,  valid: '"sqlserver" | "mssql" | "oracle" | "postgresql" | "postgres" | "s3" | "parquet" | "kafka" | "pubsub"', description: 'Target system type.' },
      { field: 'target.connection', type: 'string', required: false, description: 'Airflow connection ID for RDBMS targets. Overrides target.host/port/database.' },
      { field: 'target.schema',  type: 'string',  required: false, default: '"dbo" / "SCHEMA1"', description: 'Database schema. Defaults to "dbo" for SQL Server, connection default for Oracle/PostgreSQL.' },
      { field: 'target.table',   type: 'string',  required: true,  description: 'Destination table name.' },
      { field: 'target.database', type: 'string', required: false, description: 'Override the database name from the connection profile.' },
      { field: 'target.mode',    type: 'string',  required: false, default: '"append"', valid: '"append" | "overwrite" | "merge"', description: '"append" inserts all rows. "overwrite" truncates then inserts. "merge" matches on target.keys and updates existing rows or inserts new ones (RDBMS only).' },
      { field: 'target.keys',    type: 'string[]',required: false, description: 'Column name(s) used as the match key for merge mode. Required when mode is "merge". Example: ["order_id"] or ["pipeline_id", "run_date"].' },
      { field: 'target.bucket',  type: 'string',  required: false, description: 'S3 bucket name (S3 target only).' },
      { field: 'target.path',    type: 'string',  required: false, description: 'S3 key prefix (S3 target only).' },
      { field: 'target.topic',   type: 'string',  required: false, description: 'Kafka topic or PubSub topic name.' },
    ],
    sample: `{
  "task_id": "load_to_oracle_target",
  "type": "load_target",
  "depends_on": ["transform_enriched_orders"],
  "source": { "table": "enriched_orders" },
  "target": {
    "type": "oracle",
    "connection": "domedit_oracle",
    "schema": "SCHEMA1",
    "table": "ENRICHED_ORDERS_STAGE",
    "mode": "overwrite"
  }
}`,
  },
  {
    id: 'notification',
    label: 'notification',
    category: 'load',
    icon: Bell,
    color: 'text-yellow-600',
    description: 'Sends a notification to Slack channels or email recipients. Typically placed at the end of a pipeline to signal completion or send a summary.',
    fields: [
      { field: 'type',           type: 'string',  required: true,  valid: '"notification"', description: 'Must be "notification".' },
      { field: 'channels',       type: 'string[]',required: true,  valid: '"slack:#channel" | "email:addr@domain.com"', description: 'List of notification destinations. Each entry is a prefixed string.' },
      { field: 'subject',        type: 'string',  required: false, description: 'Email subject line. Supports {{ run_id }}, {{ ds }}, {{ pipeline_id }}.' },
      { field: 'message',        type: 'string',  required: false, description: 'Notification body text. Slack ignores the subject. Supports template variables.' },
    ],
    sample: `{
  "task_id": "notify_team",
  "type": "notification",
  "depends_on": ["load_to_oracle_target"],
  "channels": ["slack:#data-notifications", "email:data-eng@company.com"],
  "subject": "DOMEDIT ETL completed for {{ ds }}",
  "message": "Pipeline domedit_orders_etl finished successfully. Run: {{ run_id }}"
}`,
  },

  // ── Action Triggers ──────────────────────────────────────────────────────
  {
    id: 'api_call',
    label: 'api_call',
    category: 'triggers',
    icon: Webhook,
    color: 'text-rose-600',
    description: 'Calls an external HTTP endpoint. Supports all common auth schemes. Secret values can be referenced as "secret:<secret-name>/<json-key>" and are resolved from AWS Secrets Manager at runtime. Does NOT produce a DuckDB file.',
    fields: [
      { field: 'type',           type: 'string',  required: true,  valid: '"api_call"', description: 'Must be "api_call".' },
      { field: 'url',            type: 'string',  required: true,  description: 'Target URL. Supports {{ run_id }}, {{ ds }}, {{ pipeline_id }}.' },
      { field: 'method',         type: 'string',  required: false, default: '"GET"', valid: '"GET" | "POST" | "PUT" | "PATCH" | "DELETE"', description: 'HTTP verb.' },
      { field: 'headers',        type: 'object',  required: false, description: 'Additional request headers. Values support "secret:" prefix for Secrets Manager.' },
      { field: 'params',         type: 'object',  required: false, description: 'URL query parameters appended as ?key=value.' },
      { field: 'body',           type: 'object|string', required: false, description: 'Request body. Objects are JSON-serialised. Sent with Content-Type: application/json.' },
      { field: 'auth.type',      type: 'string',  required: false, default: '"none"', valid: '"none" | "basic" | "bearer" | "api_key" | "oauth2_client_credentials"', description: 'Authentication scheme.' },
      { field: 'auth.username / password', type: 'string', required: false, description: 'Credentials for basic auth. Password supports "secret:" prefix.' },
      { field: 'auth.token',     type: 'string',  required: false, description: 'Bearer token. Supports "secret:" prefix.' },
      { field: 'auth.key / value / in', type: 'string', required: false, description: 'api_key: header name, value (supports "secret:"), and location ("header" or "query").' },
      { field: 'auth.token_url / client_id / client_secret / scope', type: 'string', required: false, description: 'oauth2_client_credentials fields. client_secret supports "secret:" prefix.' },
      { field: 'assert_status',  type: 'integer[]', required: false, description: 'Acceptable HTTP status codes. Defaults to 200–299. Task fails if response code is not in this list.' },
      { field: 'retry_on_status',type: 'integer[]', required: false, description: 'Status codes that trigger an Airflow retry (e.g. [429, 503]).' },
      { field: 'timeout_seconds',type: 'integer', required: false, default: '30',    description: 'Request timeout in seconds.' },
      { field: 'response_xcom_key', type: 'string', required: false, description: 'If set, the response body (parsed as JSON when possible) is pushed to XCom under this key for use by downstream tasks.' },
    ],
    sample: `{
  "task_id": "fetch_exchange_rates",
  "type": "api_call",
  "url": "https://api.exchangerate.internal/v2/rates?base=USD&date={{ ds }}",
  "method": "GET",
  "auth": {
    "type": "api_key",
    "key": "X-Api-Key",
    "value": "secret:nextgen-databridge/connections/exchange_rate_api/api_key",
    "in": "header"
  },
  "assert_status": [200],
  "retry_on_status": [429, 503],
  "timeout_seconds": 30,
  "response_xcom_key": "exchange_rates"
}

// OAuth2 example:
{
  "task_id": "notify_downstream",
  "type": "api_call",
  "url": "https://downstream.corp/api/v1/etl/complete",
  "method": "POST",
  "auth": {
    "type": "oauth2_client_credentials",
    "token_url": "https://auth.corp/oauth2/token",
    "client_id": "nextgen-databridge",
    "client_secret": "secret:nextgen-databridge/connections/downstream_api/client_secret",
    "scope": "etl:notify"
  },
  "body": { "pipeline_id": "domedit_etl", "run_id": "{{ run_id }}", "status": "completed" },
  "assert_status": [200, 201, 202]
}`,
  },
  {
    id: 'autosys_job',
    label: 'autosys_job',
    category: 'triggers',
    icon: Calendar,
    color: 'text-orange-600',
    description: 'Triggers a CA Workload Automation (Autosys) job via the REST API. Can optionally poll for completion using the Autosys job status endpoint. Does NOT produce a DuckDB file.',
    fields: [
      { field: 'type',           type: 'string',  required: true,  valid: '"autosys_job"', description: 'Must be "autosys_job".' },
      { field: 'autosys_url',    type: 'string',  required: true,  description: 'Autosys REST API base URL, e.g. "https://autosys.corp.internal:9443".' },
      { field: 'job_name',       type: 'string',  required: true,  description: 'Autosys job name exactly as defined in the Autosys scheduler.' },
      { field: 'auth.type',      type: 'string',  required: false, default: '"autosys_token"', valid: '"autosys_token" | "bearer" | "basic"', description: 'autosys_token = POST /v1/security/tokens for a session bearer token. bearer = static token. basic = Base64(user:pass).' },
      { field: 'auth.username / password', type: 'string', required: false, description: 'For autosys_token and basic auth. Password supports "secret:" prefix.' },
      { field: 'auth.token',     type: 'string',  required: false, description: 'For bearer auth. Supports "secret:" prefix.' },
      { field: 'job_params',     type: 'object',  required: false, description: 'Key/value pairs passed as jobParameters in the trigger request. Supported only if the Autosys job is configured to accept runtime parameters.' },
      { field: 'wait_for_completion', type: 'boolean', required: false, default: 'false', description: 'If true, poll the job status until a terminal state. If false, fire-and-forget.' },
      { field: 'poll_interval_seconds', type: 'integer', required: false, default: '30', description: 'Seconds between status polls when wait_for_completion=true.' },
      { field: 'timeout_seconds', type: 'integer',required: false, default: '3600', description: 'Max wait time. Task fails if the job has not terminated within this window.' },
    ],
    sample: `{
  "task_id": "trigger_autosys_staging",
  "type": "autosys_job",
  "autosys_url": "https://autosys.corp.internal:9443",
  "job_name": "DOMEDIT_DAILY_STAGE_LOAD",
  "auth": {
    "type": "autosys_token",
    "username": "svc-databridge",
    "password": "secret:nextgen-databridge/connections/autosys/password"
  },
  "job_params": {
    "PROCESS_DATE": "{{ ds }}",
    "SOURCE_SYSTEM": "DOMEDIT"
  },
  "wait_for_completion": true,
  "poll_interval_seconds": 30,
  "timeout_seconds": 1800
}`,
  },
  {
    id: 'stored_proc',
    label: 'stored_proc',
    category: 'triggers',
    icon: GitBranch,
    color: 'text-indigo-600',
    description: 'Executes a stored procedure on SQL Server, Oracle, or PostgreSQL. Supports IN/OUT/INOUT parameters and optional result set capture to DuckDB on S3. Oracle uses anonymous PL/SQL blocks with cursor.var() OUT parameters; REF CURSOR output parameters become the captured result set.',
    fields: [
      { field: 'type',           type: 'string',  required: true,  valid: '"stored_proc"', description: 'Must be "stored_proc".' },
      { field: 'connection',     type: 'string',  required: true,  description: 'Airflow connection ID. Determines the database driver used (mssql → pyodbc, oracle → oracledb/cx_Oracle, other → psycopg2).' },
      { field: 'procedure',      type: 'string',  required: true,  description: 'Fully-qualified procedure name, e.g. "Sales.usp_GenerateSummary" for SQL Server or "SCHEMA1.PKG.PROC" for Oracle.' },
      { field: 'parameters',     type: 'Param[]', required: false, description: 'List of parameter objects.' },
      { field: 'parameters[].name', type: 'string', required: true, description: 'Parameter name (without @ for SQL Server, without : for Oracle).' },
      { field: 'parameters[].value', type: 'any',  required: false, description: 'Input value. Use null for OUT parameters.' },
      { field: 'parameters[].type', type: 'string', required: false, description: 'Type hint used for Oracle cursor.var() binding. Options: "int", "number", "float", "varchar", "date", "timestamp", "clob", "blob", "refcursor".' },
      { field: 'parameters[].direction', type: 'string', required: false, default: '"in"', valid: '"in" | "out" | "inout"', description: 'Parameter direction. OUT parameters are returned via XCom as output_parameters dict.' },
      { field: 'capture_resultset', type: 'boolean', required: false, default: 'false', description: 'If true, the first result set (SQL Server/PostgreSQL) or REF CURSOR OUT param (Oracle) is written to a DuckDB file on S3.' },
      { field: 'output.duckdb_path', type: 'string', required: false, description: 'S3 path for result set DuckDB. Auto-generated if capture_resultset=true and path is omitted.' },
      { field: 'output.table',   type: 'string',  required: false, default: '"result"', description: 'Table name inside the result set DuckDB file.' },
    ],
    sample: `// SQL Server example — with OUTPUT parameters
{
  "task_id": "proc_daily_summary",
  "type": "stored_proc",
  "connection": "wwi_sqlserver",
  "procedure": "Sales.usp_GenerateDailySalesSummary",
  "parameters": [
    { "name": "ProcessDate",  "value": "{{ ds }}", "direction": "in"  },
    { "name": "CurrencyCode", "value": "USD",       "direction": "in"  },
    { "name": "RowsInserted", "value": null,        "direction": "out" },
    { "name": "ErrorMessage", "value": null,        "direction": "out" }
  ],
  "capture_resultset": true,
  "output": { "table": "daily_summary" }
}

// Oracle example — REF CURSOR result set
{
  "task_id": "proc_oracle_report",
  "type": "stored_proc",
  "connection": "domedit_oracle",
  "procedure": "SCHEMA1.PKG_REPORTS.GET_DAILY_ORDERS",
  "parameters": [
    { "name": "P_DATE",   "value": "{{ ds }}", "direction": "in"  },
    { "name": "P_CURSOR", "value": null,        "type": "refcursor", "direction": "out" }
  ],
  "capture_resultset": true,
  "output": { "table": "oracle_daily_orders" }
}

// PostgreSQL example
{
  "task_id": "proc_pg_recalc",
  "type": "stored_proc",
  "connection": "audit_db",
  "procedure": "analytics.recalculate_metrics",
  "parameters": [
    { "name": "p_date",         "value": "{{ ds }}", "direction": "in"    },
    { "name": "p_rows_updated", "value": null,        "direction": "out"   }
  ],
  "capture_resultset": false
}`,
  },
  // ── Control Flow ────────────────────────────────────────────────────────
  {
    id: 'conditional_branch',
    label: 'conditional_branch',
    category: 'control',
    icon: GitBranch,
    color: 'text-fuchsia-600',
    description: 'Airflow BranchPythonOperator wrapper that evaluates a Python expression and routes execution to one of several downstream branches. Upstream XCom values are available in the expression by task_id name. Only the chosen branch task runs; all others are skipped.',
    fields: [
      { field: 'type',              type: 'string',  required: true,  valid: '"conditional_branch"', description: 'Must be "conditional_branch".' },
      { field: 'depends_on',        type: 'string[]',required: false, description: 'Upstream task IDs to wait for before evaluating the expression. Their XCom values are injected as local variables.' },
      { field: 'condition_expression', type: 'string', required: true, description: 'Python expression evaluated in a restricted sandbox (no builtins). Must evaluate to a string matching a branch label or condition. XCom values from depends_on tasks are available by task_id name.' },
      { field: 'branches',          type: 'Branch[]',required: true,  description: 'List of possible branch objects.' },
      { field: 'branches[].label',  type: 'string',  required: true,  description: 'The value condition_expression must equal to select this branch. Use "default" for the fallback branch when no other label matches.' },
      { field: 'branches[].task_id',type: 'string',  required: true,  description: 'task_id of the downstream task to run when this branch is selected. That task must appear elsewhere in the tasks array with a depends_on pointing back to this conditional_branch task.' },
      { field: 'branches[].condition', type: 'string', required: false, description: 'Alternative match string checked if label does not match. Useful when you want both a human label and a machine value.' },
    ],
    sample: `// Route by data volume: big → EKS heavy transform, small → in-process DuckDB
{
  "task_id": "branch_by_volume",
  "type": "conditional_branch",
  "depends_on": ["extract_domedit_orders"],
  "condition_expression": "'large' if int((extract_domedit_orders or {}).get('row_count', 0)) > 500000 else 'small'",
  "branches": [
    { "label": "large", "task_id": "heavy_transform_eks" },
    { "label": "small", "task_id": "light_transform_duckdb" },
    { "label": "default", "task_id": "light_transform_duckdb" }
  ]
}

// The downstream tasks reference back:
{
  "task_id": "heavy_transform_eks",
  "type": "eks_job",
  "depends_on": ["branch_by_volume"],
  "trigger_rule": "none_failed_min_one_success",
  "execution": { "image": "...ecr.../nextgen-databridge-jobs:latest", "cpu": "8", "memory": "32Gi" }
}

{
  "task_id": "light_transform_duckdb",
  "type": "duckdb_transform",
  "depends_on": ["extract_domedit_orders", "branch_by_volume"],
  "trigger_rule": "none_failed_min_one_success",
  "sql": "SELECT * FROM extract_domedit_orders.raw_orders"
}`,
  },
]

// ── Field table ───────────────────────────────────────────────────────────────
function FieldTable({ fields }: { fields: FieldDef[] }) {
  return (
    <div className="overflow-x-auto rounded-lg border border-gray-200">
      <table className="w-full text-xs">
        <thead>
          <tr className="bg-gray-50 border-b border-gray-200">
            <th className="text-left px-3 py-2.5 font-semibold text-gray-700 w-48">Field</th>
            <th className="text-left px-3 py-2.5 font-semibold text-gray-700 w-24">Type</th>
            <th className="text-left px-3 py-2.5 font-semibold text-gray-700 w-20">Required</th>
            <th className="text-left px-3 py-2.5 font-semibold text-gray-700 w-28">Default</th>
            <th className="text-left px-3 py-2.5 font-semibold text-gray-700 w-52">Valid values</th>
            <th className="text-left px-3 py-2.5 font-semibold text-gray-700">Description</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100">
          {fields.map(f => (
            <tr key={f.field} className="hover:bg-gray-50/60 transition-colors">
              <td className="px-3 py-2.5 align-top">
                <code className="text-blue-700 font-mono text-[11px] bg-blue-50 px-1.5 py-0.5 rounded">
                  {f.field}
                </code>
              </td>
              <td className="px-3 py-2.5 align-top">
                <span className="font-mono text-[11px] text-violet-600">{f.type}</span>
              </td>
              <td className="px-3 py-2.5 align-top">
                {f.required
                  ? <span className="text-red-600 font-semibold">yes</span>
                  : <span className="text-gray-400">no</span>}
              </td>
              <td className="px-3 py-2.5 align-top text-gray-500 font-mono text-[11px]">
                {f.default ?? '—'}
              </td>
              <td className="px-3 py-2.5 align-top text-gray-600 font-mono text-[11px] break-words max-w-[200px]">
                {f.valid ?? '—'}
              </td>
              <td className="px-3 py-2.5 align-top text-gray-600 leading-relaxed">
                {f.description}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// ── Copy button ───────────────────────────────────────────────────────────────
function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false)
  function handleCopy() {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    })
  }
  return (
    <button
      onClick={handleCopy}
      className="flex items-center gap-1.5 text-xs text-gray-500 hover:text-gray-800 transition-colors px-2 py-1 rounded hover:bg-gray-100"
    >
      {copied
        ? <><CheckCircle2 size={13} className="text-emerald-500" /> Copied</>
        : <><Copy size={13} /> Copy</>}
    </button>
  )
}

// ── Main page ─────────────────────────────────────────────────────────────────
export default function ConfigReference() {
  const [selectedId,  setSelectedId]  = useState('pipeline_envelope')
  const [searchQuery, setSearchQuery] = useState('')
  const [activeCategory, setActiveCategory] = useState<string | null>(null)

  const filtered = useMemo(() => {
    const q = searchQuery.toLowerCase()
    return TASK_DEFS.filter(t => {
      const matchesSearch = !q || t.label.toLowerCase().includes(q) || t.description.toLowerCase().includes(q) || t.category.toLowerCase().includes(q)
      const matchesCat    = !activeCategory || t.category === activeCategory
      return matchesSearch && matchesCat
    })
  }, [searchQuery, activeCategory])

  const selected = TASK_DEFS.find(t => t.id === selectedId) ?? TASK_DEFS[0]

  // Auto-select first visible item when filter changes
  const firstFiltered = filtered[0]
  const selectedVisible = filtered.some(t => t.id === selectedId)

  React.useEffect(() => {
    if (!selectedVisible && firstFiltered) setSelectedId(firstFiltered.id)
  }, [selectedVisible, firstFiltered?.id])

  return (
    <div className="flex h-full overflow-hidden bg-gray-50">

      {/* ── Left panel ───────────────────────────────────────────────────── */}
      <div className="w-64 flex-shrink-0 border-r border-gray-200 bg-white flex flex-col overflow-hidden">

        {/* Header */}
        <div className="px-4 py-4 border-b border-gray-100">
          <div className="flex items-center gap-2 mb-3">
            <BookOpen size={16} className="text-blue-600 flex-shrink-0" />
            <h2 className="text-sm font-bold text-gray-900">Config Reference</h2>
          </div>
          <div className="relative">
            <Search size={13} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-400" />
            <input
              value={searchQuery}
              onChange={e => setSearchQuery(e.target.value)}
              placeholder="Search…"
              className="w-full bg-gray-50 border border-gray-200 rounded-lg pl-7 pr-3 py-1.5 text-xs text-gray-900 focus:outline-none focus:ring-2 focus:ring-blue-500 placeholder:text-gray-400"
            />
          </div>
        </div>

        {/* Category filter chips */}
        <div className="px-3 py-2 border-b border-gray-100 flex flex-wrap gap-1">
          <button
            onClick={() => setActiveCategory(null)}
            className={clsx(
              'px-2 py-0.5 rounded-full text-[11px] font-medium transition-colors',
              !activeCategory ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200',
            )}
          >All</button>
          {CATEGORIES.map(c => (
            <button
              key={c.id}
              onClick={() => setActiveCategory(activeCategory === c.id ? null : c.id)}
              className={clsx(
                'px-2 py-0.5 rounded-full text-[11px] font-medium transition-colors',
                activeCategory === c.id ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200',
              )}
            >{c.label}</button>
          ))}
        </div>

        {/* Task list */}
        <div className="flex-1 overflow-y-auto py-2">
          {CATEGORIES.map(cat => {
            const items = filtered.filter(t => t.category === cat.id)
            if (!items.length) return null
            const CatIcon = cat.icon
            return (
              <div key={cat.id} className="mb-1">
                <div className="flex items-center gap-1.5 px-4 py-1.5">
                  <CatIcon size={11} className="text-gray-400" />
                  <span className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider">{cat.label}</span>
                </div>
                {items.map(t => {
                  const TIcon = t.icon
                  return (
                    <button
                      key={t.id}
                      onClick={() => setSelectedId(t.id)}
                      className={clsx(
                        'w-full flex items-center gap-2.5 px-4 py-2 text-left text-xs transition-colors',
                        selectedId === t.id
                          ? 'bg-blue-50 text-blue-700 font-semibold border-r-2 border-blue-500'
                          : 'text-gray-600 hover:bg-gray-50 hover:text-gray-900',
                      )}
                    >
                      <TIcon size={13} className={clsx('flex-shrink-0', t.color)} />
                      <span className="truncate font-mono">{t.label}</span>
                    </button>
                  )
                })}
              </div>
            )
          })}
          {filtered.length === 0 && (
            <p className="text-xs text-gray-400 text-center py-8 px-4">No matches for "{searchQuery}"</p>
          )}
        </div>
      </div>

      {/* ── Right panel ──────────────────────────────────────────────────── */}
      <div className="flex-1 overflow-y-auto">
        <div className="max-w-4xl mx-auto px-8 py-8 space-y-8">

          {/* Title */}
          <div className="flex items-start gap-3">
            {React.createElement(selected.icon, {
              size: 24,
              className: clsx('flex-shrink-0 mt-0.5', selected.color),
            })}
            <div>
              <h1 className="text-xl font-bold text-gray-900 font-mono">{selected.label}</h1>
              <p className="text-sm text-gray-600 mt-1 leading-relaxed max-w-2xl">{selected.description}</p>
            </div>
          </div>

          {/* Sample config */}
          <div>
            <div className="flex items-center justify-between mb-2">
              <h2 className="text-xs font-semibold text-gray-700 uppercase tracking-wider">Sample Configuration</h2>
              <CopyButton text={selected.sample} />
            </div>
            <div className="rounded-xl overflow-hidden border border-gray-200 shadow-sm">
              <Editor
                height="320px"
                language="json"
                value={selected.sample}
                theme="vs"
                options={{
                  readOnly: true,
                  minimap: { enabled: false },
                  scrollBeyondLastLine: false,
                  fontSize: 12,
                  lineNumbers: 'off',
                  folding: true,
                  wordWrap: 'on',
                  scrollbar: { vertical: 'auto', horizontal: 'hidden' },
                  padding: { top: 12, bottom: 12 },
                  renderLineHighlight: 'none',
                  overviewRulerLanes: 0,
                  hideCursorInOverviewRuler: true,
                }}
              />
            </div>
          </div>

          {/* Field reference */}
          <div>
            <h2 className="text-xs font-semibold text-gray-700 uppercase tracking-wider mb-2">
              Field Reference
            </h2>
            <FieldTable fields={selected.fields} />
          </div>

          {/* Navigation pills */}
          <div className="flex items-center justify-between pt-4 border-t border-gray-100">
            {(() => {
              const idx  = TASK_DEFS.findIndex(t => t.id === selected.id)
              const prev = TASK_DEFS[idx - 1]
              const next = TASK_DEFS[idx + 1]
              return (
                <>
                  {prev
                    ? <button onClick={() => setSelectedId(prev.id)} className="flex items-center gap-1.5 text-xs text-gray-500 hover:text-blue-600 transition-colors">
                        <ChevronRight size={13} className="rotate-180" />
                        <span className="font-mono">{prev.label}</span>
                      </button>
                    : <span />}
                  {next
                    ? <button onClick={() => setSelectedId(next.id)} className="flex items-center gap-1.5 text-xs text-gray-500 hover:text-blue-600 transition-colors">
                        <span className="font-mono">{next.label}</span>
                        <ChevronRight size={13} />
                      </button>
                    : <span />}
                </>
              )
            })()}
          </div>

        </div>
      </div>
    </div>
  )
}
