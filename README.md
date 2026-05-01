# NextGenDatabridge Platform

A production-grade **ELT pipeline platform** built on Apache Airflow (MWAA), DuckDB, FastAPI, and React. Orchestrate complex data pipelines from SQL Server, Oracle, Kafka, Pub/Sub, and file sources to any target — with full audit trails, data quality checks, deployment approvals, and a live admin UI.

---

## Architecture

```
                        ┌─────────────────────────────────────────────────────┐
                        │                     AWS (us-east-1)                  │
                        │                                                       │
  GitHub Actions ──────►│  ECR  ──────────────────────────────────────────┐   │
  (CI/CD)               │  (api, ui, transform images)                    │   │
                        │                                                  ▼   │
                        │  ┌────────────────────────────────────────────────┐ │
                        │  │                EKS Cluster                     │ │
                        │  │  system node group (m5.large × 2-4)           │ │
                        │  │  ┌──────────────┐  ┌──────────────────────┐  │ │
                        │  │  │  API (FastAPI)│  │  UI (React/nginx)    │  │ │
                        │  │  └──────────────┘  └──────────────────────┘  │ │
                        │  │                                                │ │
                        │  │  jobs node group (r5.xlarge SPOT × 0-20)     │ │
                        │  │  ┌───────────────────────────────────────┐   │ │
                        │  │  │  transform_job.py  (heavy EKS Jobs)   │   │ │
                        │  │  └───────────────────────────────────────┘   │ │
                        │  └────────────────────────────────────────────────┘ │
                        │                                                       │
                        │  ┌──────────────────────────────────────────────┐   │
                        │  │       MWAA (Apache Airflow 2.x)              │   │
                        │  │  dag_generator.py → custom operators         │   │
                        │  │  Most tasks run here on Airflow workers       │   │
                        │  │  eks_job tasks → submit K8s Job → poll       │   │
                        │  └──────────────────────────────────────────────┘   │
                        │                      │                               │
                        │         ┌────────────▼───────────────┐              │
                        │         │            S3               │              │
                        │         │  duckdb-store  (pipeline    │              │
                        │         │  intermediate DuckDB files) │              │
                        │         │  pipeline-configs           │              │
                        │         │  artifacts  (SQL backups)   │              │
                        │         └────────────────────────────┘              │
                        │                                                       │
                        │  RDS PostgreSQL  ──  RDS SQL Server (WWI)           │
                        │  ElastiCache Redis  (Airflow Celery broker)         │
                        │  Secrets Manager    (all connection credentials)    │
                        └─────────────────────────────────────────────────────┘
```

---

## Infrastructure Components

| Component | Service | Details |
|-----------|---------|---------|
| Orchestration | AWS MWAA | Managed Airflow, CeleryExecutor, private VPC |
| Container platform | Amazon EKS 1.31 | System + SPOT jobs node groups |
| API / UI | EKS Deployments | FastAPI + React served via ALB |
| Heavy transforms | EKS Kubernetes Jobs | SPOT r5.xlarge instances, `transform_job.py` |
| Source DB (WWI) | RDS SQL Server SE 15.0 | WideWorldImporters, publicly accessible |
| Audit / Airflow DB | RDS PostgreSQL 15 | Publicly accessible |
| Celery broker | ElastiCache Redis 7.0 | TLS enabled, private subnets |
| Intermediate storage | S3 | DuckDB files, pipeline configs, artifacts |
| Image registry | ECR | api, ui, transform repos |
| Credentials | Secrets Manager | All DB connections under `nextgen-databridge/*` |

---

## Deployment

### GitHub Actions Workflows

All infrastructure and application deployments run through GitHub Actions using AWS OIDC (no long-lived credentials).

#### `infra-provision.yml` — On-demand infrastructure management

Triggered manually via **Actions → Infrastructure Provision → Run workflow**.

```
Inputs:
  environment:  dev | staging | production | dr
  module:       all | core | mwaa | wwi
  action:       plan | apply | destroy
```

Job sequence when `module = all`:

```
setup
  │
  ├─► bootstrap        (S3 state bucket + DynamoDB lock table)
  │
  ├─► plan-core        (terraform plan infra/terraform/core)
  │
  └─► apply-core       (requires GitHub Environment approval for staging/prod)
        │
        ├─► plan-mwaa  (runs AFTER apply-core — data sources need live VPC/EKS)
        ├─► plan-wwi
        │
        ├─► apply-mwaa (requires approval)
        └─► apply-wwi  (requires approval)
```

Plan output is posted to the job summary so you can review before approving apply.

#### `deploy-dev.yml` — Application deployment to dev

Triggers:
- Automatically on PR merge to `main`
- On-demand via **Actions → Deploy Dev → Run workflow** (with optional reason field)

Builds and pushes Docker images to ECR, then rolls out to EKS.

#### `ci.yml` — Pull request checks

Runs on every PR: Python tests, linting, and `terraform validate` (all three modules).

### Required Setup

**Repository secrets** (Settings → Secrets and variables → Actions):
- `DB_PASSWORD` — PostgreSQL password
- `MSSQL_PASSWORD` — SQL Server password

**GitHub Environments** (Settings → Environments):
- `dev`, `staging`, `production`, `dr`
- Add Required Reviewers to `staging`/`production`/`dr` to gate apply/destroy

IAM role ARNs are stored as workflow `env:` vars (not secrets) in `infra-provision.yml` — update them when creating roles in new accounts.

---

## Task Execution: Airflow vs EKS

This is the key architectural decision for every pipeline task.

### How the decision is made

The `type` field in each task config determines where it runs:

| `type` value | Runs on | Mechanism |
|---|---|---|
| `eks_job` | **EKS SPOT Job** | `EKSJobOperator` submits a `batch/v1 Job`, polls until complete |
| everything else | **MWAA Airflow worker** | Python operator runs inline on the Celery worker |

### When to use `eks_job`

Use `eks_job` for transforms that are:
- Memory-intensive (joining multiple large DuckDB files)
- Long-running (minutes to hours)
- Bursty (no point keeping dedicated workers warm for them)

The SPOT job pool scales to 20 nodes and scales back to zero when idle, so heavy transforms don't block or slow Airflow workers.

All other task types (`sql_extract`, `data_quality`, `load_target`, etc.) run directly on the MWAA worker — they are fast, low-memory operations that don't justify the K8s Job overhead.

### EKSJobOperator flow

```
Airflow worker
    │
    │  1. Submits batch/v1 Job to EKS
    │     - Image: ECR/nextgen-databridge/transform:<sha>
    │     - Node selector: role=nextgen-databridge-job (SPOT pool)
    │     - Env: PIPELINE_ID, RUN_ID, TASK_ID, INPUT_PATHS (JSON),
    │            OUTPUT_DUCKDB_PATH, TASK_CONFIG (JSON)
    │
    │  2. Polls every 15 s until Job completes or fails
    │
    │  3. On success: reads OUTPUT_DUCKDB_PATH from XCom
    │     so downstream tasks can reference the result file
    │
    ▼
EKS SPOT Pod (transform_job.py)
    │
    │  1. Downloads input DuckDB files from S3 to /tmp
    │  2. ATTACHes each as a read-only alias in DuckDB
    │  3. Runs the SQL from task config
    │     CREATE OR REPLACE TABLE <output_table> AS (<sql>)
    │  4. Uploads result DuckDB to S3 (OUTPUT_DUCKDB_PATH)
    │  5. Writes task_run record to audit DB (success/failed)
    │  6. Exits 0 (success) or 1 (failure)
```

### Example: `eks_job` task config

```json
{
  "task_id": "heavy_aggregation",
  "type": "eks_job",
  "depends_on": ["extract_orders", "extract_products"],
  "sql": "SELECT o.order_id, p.category, SUM(o.amount) AS total FROM extract_orders.raw AS o JOIN extract_products.raw AS p ON o.product_id = p.id GROUP BY 1, 2",
  "output": {
    "duckdb_path": "s3://nextgen-databridge-duckdb-store-dev/pipelines/my_pipeline/runs/{{ run_id }}/aggregated.duckdb",
    "table": "aggregated"
  }
}
```

The `INPUT_PATHS` env var is automatically populated by the operator from the XCom values of the `depends_on` tasks.

---

## Pipeline Configuration

Pipelines are defined as JSON documents stored in S3 (`nextgen-databridge-pipeline-configs-<env>`). The dynamic DAG generator reads all active configs and creates Airflow DAGs automatically.

### Full example

```json
{
  "pipeline_id": "wwi_orders_etl",
  "version": "1.0.0",
  "schedule": "0 6 * * *",
  "retries": 3,
  "sla_minutes": 90,
  "alerting": {
    "on_failure": ["email:team@company.com", "slack:#data-alerts"]
  },
  "tasks": [
    {
      "task_id": "extract",
      "type": "sql_extract",
      "source": {
        "connection": "wwi_sqlserver",
        "query": "SELECT * FROM Sales.Orders WHERE OrderDate >= '{{ ds }}'"
      },
      "output": {
        "duckdb_path": "s3://nextgen-databridge-duckdb-store-dev/pipelines/wwi_orders_etl/runs/{{ run_id }}/extract.duckdb",
        "table": "raw_orders"
      }
    },
    {
      "task_id": "transform",
      "type": "eks_job",
      "depends_on": ["extract"],
      "sql": "SELECT CustomerID, COUNT(*) AS order_count, SUM(OrderTotal) AS total FROM extract.raw_orders GROUP BY CustomerID",
      "output": {
        "duckdb_path": "s3://nextgen-databridge-duckdb-store-dev/pipelines/wwi_orders_etl/runs/{{ run_id }}/transform.duckdb",
        "table": "customer_summary"
      }
    },
    {
      "task_id": "quality_check",
      "type": "data_quality",
      "depends_on": ["transform"],
      "checks": [
        { "type": "not_null", "column": "CustomerID", "action": "fail" },
        { "type": "row_count_min", "value": 1, "action": "fail" }
      ]
    },
    {
      "task_id": "load",
      "type": "load_target",
      "depends_on": ["quality_check"],
      "source": { "table": "customer_summary" },
      "target": {
        "type": "mssql",
        "connection": "wwi_sqlserver_target",
        "table": "dbo.CustomerOrderSummary",
        "write_mode": "replace"
      }
    }
  ]
}
```

### Running any task on EKS

Add `"engine": "eks"` to any `sql_extract` or transform task to run it on an EKS SPOT Job instead of an Airflow worker. The container automatically detects what to do from `OPERATION_TYPE`:

```json
{ "task_id": "extract_orders", "type": "sql_extract", "engine": "eks",
  "execution": { "memory": "16Gi", "cpu": "4" },
  "source": { "connection": "wwi_sqlserver", "query": "SELECT * FROM Sales.Orders" },
  "output": { "duckdb_path": "s3://...extract.parquet" }
}
```

```json
{ "task_id": "transform_summary", "type": "duckdb_transform", "engine": "eks",
  "execution": { "memory": "32Gi", "cpu": "8" },
  "sql": "SELECT CustomerID, SUM(total) FROM extract.raw GROUP BY 1",
  "output": { "duckdb_path": "s3://...transform.duckdb", "table": "summary" }
}
```

Use this for tables with millions–billions of rows where running on an Airflow worker would be too slow or exhaust memory. Extract tasks write Parquet to S3 (streamed in 500k-row chunks — no full dataset in memory). Transform tasks write DuckDB to S3.

### Task types reference

| Type | Default runner | Description |
|------|----------------|-------------|
| `sql_extract` | Airflow worker | Extract from SQL Server, Oracle, PostgreSQL via SQL query |
| `duckdb_transform` | Airflow worker | SQL transform using DuckDB (joins multiple upstream DuckDB files) |
| `eks_job` | **EKS SPOT Job** | Heavy SQL transform in isolated Kubernetes Job container |
| `data_quality` | Airflow worker | Run configurable QC checks (not_null, unique, row_count, freshness, regex, custom_sql) |
| `schema_validate` | Airflow worker | Validate column names and types against expected schema |
| `file_ingest` | Airflow worker | Ingest CSV/TSV/Parquet/JSON/Excel from S3 or local path |
| `kafka_consume` | Airflow worker | Consume messages from Kafka topic into DuckDB |
| `kafka_produce` | Airflow worker | Publish DuckDB data to Kafka topic |
| `pubsub_consume` | Airflow worker | Pull messages from GCP Pub/Sub subscription |
| `pubsub_publish` | Airflow worker | Publish to GCP Pub/Sub topic |
| `load_target` | Airflow worker | Load DuckDB data to SQL Server, Oracle, PostgreSQL, S3, Kafka, or Pub/Sub |
| `conditional_branch` | Airflow worker | Branch pipeline based on evaluated expression |
| `notification` | Airflow worker | Send Slack / email notifications |
| `cdc_extract` | Airflow worker | Incremental extract using change tracking |
| `api_call` | Airflow worker | Call an external REST API and optionally store the response as DuckDB |
| `autosys_job` | Airflow worker | Trigger and monitor a CA Autosys job, poll until terminal state |
| `stored_proc` | Airflow worker | Execute a stored procedure on SQL Server / Oracle and optionally capture resultsets |

### QC check types

| Check | Description |
|-------|-------------|
| `not_null` | Count null values in column — must be 0 |
| `unique` | Check for duplicate values |
| `row_count_min` | Minimum number of rows |
| `row_count_max` | Maximum number of rows |
| `freshness` | Most recent timestamp must be within N hours |
| `value_range` | Values must be within [min, max] |
| `regex_match` | Values must match regex pattern |
| `schema_match` | Expected columns and types must be present |
| `custom_sql` | Custom SQL that must return 0 rows |
| `referential_integrity` | Referential integrity check |

---

## Connections

All connection credentials are stored in AWS Secrets Manager under `nextgen-databridge/connections/<name>`.

| Secret name | Type | Notes |
|---|---|---|
| `nextgen-databridge/connections/wwi_sqlserver` | mssql | WideWorldImporters source DB |
| `nextgen-databridge/connections/wwi_sqlserver_target` | mssql | Target DB (TargetDB schema) |
| `nextgen-databridge/connections/audit_db` | postgresql | Airflow + audit database |
| `nextgen-databridge/connections/redis` | redis | Celery broker (TLS) |

RDS instances are publicly accessible from any IP (dev only):
- **PostgreSQL**: port 5432
- **SQL Server**: port 1433

---

## Supported Sources and Targets

### Sources
| Type | Details |
|------|---------|
| SQL Server | ODBC / pymssql, full-load and incremental |
| Oracle | cx_Oracle, full-load and incremental |
| PostgreSQL | psycopg2, full-load and incremental |
| MySQL | SQLAlchemy |
| Kafka | kafka-python consumer, configurable offset/group |
| GCP Pub/Sub | google-cloud-pubsub subscriber |
| Files | CSV, TSV, Parquet, JSON/JSONL, Excel from S3 or local |
| CDC | Change data capture via incremental SQL extracts |

### Targets
| Type | Details |
|------|---------|
| SQL Server | Bulk-insert via SQLAlchemy |
| Oracle | Bulk-insert via cx_Oracle |
| PostgreSQL | COPY-style via SQLAlchemy |
| S3 / Parquet | pyarrow |
| Kafka | kafka-python producer |
| GCP Pub/Sub | google-cloud-pubsub publisher |

---

## Monitoring & Admin UI

The React admin UI (`frontend/`) is a **read-only monitoring tool** with four pages:

| Page | URL | Purpose |
|------|-----|---------|
| Dashboard | `/` | KPI cards (active runs, success rate, rows processed today), hourly throughput chart, DAG run timeline |
| Pipeline Runs | `/runs` | Filterable table of all pipeline runs; per-row **Refresh from Airflow** button syncs latest run and task statuses |
| Run Detail | `/runs/:runId` | Full run summary with task-by-task status, durations, DuckDB output paths, and QC results; **Refresh from Airflow** button force-syncs run and all task statuses from MWAA |
| Query Explorer | `/query` | Execute ad-hoc SELECT queries against any DuckDB output file stored in S3 |

The UI connects to the FastAPI backend via the Axios client at `frontend/src/api/client.ts`. All write operations (pipeline CRUD, deployments, connections) are performed via the REST API or CI/CD workflows — the UI is intentionally read-only.

---

## Audit System

Every event is recorded in the `audit_logs` table in PostgreSQL:

- Pipeline created/updated/paused/resumed
- Run started/completed/failed/cancelled
- Task started/completed/failed/retried/skipped
- Config versions updated, connections created/tested
- Deployment submitted/approved/rejected

Every `task_runs` record captures:
- Input sources (connection, query, path)
- Output DuckDB S3 path, table name, row count, schema
- QC results (per-check pass/fail detail)
- EKS Job name and pod name (for `eks_job` tasks)
- Duration, attempts, error traceback
- Worker hostname

---

## REST API

Interactive docs are served by the running backend at `/api/docs` (Swagger UI) and `/api/redoc` (ReDoc).

| Endpoint group | Description |
|----------------|-------------|
| `GET/POST /api/pipelines` | List, create, update pipelines; pause, resume, trigger runs |
| `GET /api/runs` | List all pipeline runs with filters |
| `GET /api/runs/{run_id}` | Run detail with task list; passively syncs Airflow state when status is `running` |
| `POST /api/runs/{run_id}/sync` | Force-sync run and all task statuses from Airflow regardless of current DB state |
| `POST /api/runs/{run_id}/rerun` | Clear and re-trigger a specific task (single, downstream, or full re-run) |
| `POST /api/config/validate` | Validate a pipeline config JSON before saving |
| `GET /api/pipelines/{id}/configs` | List all config versions for a pipeline |
| `GET /api/query/duckdb-files` | List DuckDB output files available for querying |
| `POST /api/query` | Execute a read-only SQL query against a DuckDB S3 file |
| `GET /api/metrics/dashboard` | KPI counters for the dashboard |
| `GET /api/metrics/throughput` | Hourly run counts and row throughput |
| `GET /api/audit` | Full audit event log with filters |
| `GET/POST /api/connections` | Manage data source/target connections |
| `GET/POST /api/deployments` | Deployment approval workflow |
| `GET /api/eks/jobs` | List EKS Kubernetes Job records |
| `GET /health` | Health check (used by ALB target group) |

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Orchestration | AWS MWAA (Apache Airflow 2.x, CeleryExecutor) |
| Container platform | Amazon EKS 1.31 |
| Heavy transforms | Kubernetes Jobs on SPOT instances (`transform_job.py`) |
| Transform engine | DuckDB |
| Intermediate storage | S3 (`nextgen-databridge-duckdb-store-<env>`) |
| API | FastAPI + SQLAlchemy async |
| Frontend | React 18 + Vite + Tailwind CSS + Recharts |
| Auth | JWT (RS256 in prod, HS256 in dev) |
| Source database | RDS SQL Server SE 15.0 (WideWorldImporters) |
| Audit / Airflow DB | RDS PostgreSQL 15 |
| Task queue | Celery + ElastiCache Redis 7.0 |
| Image registry | Amazon ECR |
| Credentials | AWS Secrets Manager |
| IaC | Terraform (3 modules: core, mwaa, wwi) |
| CI/CD | GitHub Actions + AWS OIDC (no long-lived keys) |
| Tests | pytest + pytest-asyncio |

---

## Project Structure

```
nextgen-databridge/
├── .github/workflows/
│   ├── ci.yml                  # PR checks: tests, lint, terraform validate
│   ├── infra-provision.yml     # On-demand: plan/apply/destroy any env+module
│   └── deploy-dev.yml          # On PR merge or manual: build+push+deploy to dev
├── airflow/
│   ├── dags/
│   │   ├── dag_generator.py    # Dynamic DAG generator (reads all active configs)
│   │   └── operators/          # Custom Airflow operators (EKSJobOperator, etc.)
│   └── plugins/                # Airflow plugins directory
├── backend/
│   ├── api/main.py             # FastAPI app (all routes, WebSocket, auth)
│   ├── models/                 # SQLAlchemy ORM models
│   ├── services/               # Audit logger, config validator
│   └── migrations/             # Alembic DB migrations
├── frontend/src/               # React 18 admin UI
├── eks/
│   └── jobs/
│       ├── transform_job.py    # EKS Job entrypoint for heavy transforms
│       └── Dockerfile
├── infra/terraform/
│   ├── core/                   # VPC, EKS, S3, RDS PostgreSQL, Redis, ECR, IAM
│   ├── mwaa/                   # MWAA environment, S3 bucket, IAM
│   └── wwi/                    # RDS SQL Server, option group, IAM, secrets
├── scripts/
│   ├── create_github_role.py   # One-time: creates IAM OIDC role for GitHub Actions
│   └── sync_mwaa_runs.py       # Backfill pipeline_runs from MWAA DAG run history
└── tests/
    ├── unit/                   # Config validator unit tests
    └── integration/            # FastAPI integration tests
```
