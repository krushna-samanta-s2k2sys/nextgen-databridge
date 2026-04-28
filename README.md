
<!-- Script	When to run	Command
Deploy	Destroy	Does
./up-core.sh dev	./down-core.sh dev	Core infra + uploads env/pipeline configs
./up-wwi.sh dev	./down-wwi.sh dev	SQL Server + optional WWI restore
./up-mwaa.sh dev	./down-mwaa.sh dev	MWAA + DAG upload
./up-apps.sh dev	./down-apps.sh dev	Docker build/push + EKS deploy
./up.sh dev and ./down.sh dev remain as full orchestrators calling the 4 individual scripts in the correct order.

Test

up.sh	Every deploy	DB_PASSWORD=ActionDag!1 MSSQL_PASSWORD=ActionDag!1 bash up.sh
restore-wwi.sh	First time only	MSSQL_PASSWORD=ActionDag!1 bash restore-wwi.sh
down.sh	Teardown	DB_PASSWORD=ActionDag!1 MSSQL_PASSWORD=ActionDag!1 bash down.sh -->

$env:DB_PASSWORD="ActionDag!1"; $env:MSSQL_PASSWORD="ActionDag!1"; bash ./down.sh

<!-- # 1. Provision everything
cd infra/terraform
terraform apply -var mssql_password=ActionDag!1 -var db_password=ActionDag!1

# 2. Restore WideWorldImporters
cd ../scripts
./restore_wwi.sh $(terraform -chdir=../terraform output -raw sqlserver_endpoint) ActionDag!1

# 3. Deploy DAGs + plugins to MWAA
MWAA_BUCKET=$(terraform -chdir=infra/terraform output -raw mwaa_bucket) ./airflow/deploy_to_mwaa.sh

# 4. Tear down (when done)
cd infra/terraform && ./destroy.sh -->

<!-- API	http://a31c9466fdd1a477f9ac707114bc62fd-00de8610e187c14e.elb.us-east-1.amazonaws.com
API Docs (Swagger)	http://a31c9466fdd1a477f9ac707114bc62fd-00de8610e187c14e.elb.us-east-1.amazonaws.com/api/docs
UI	http://a3b258c43e3db4e38a8ec35d7c706117-a7780732535836a6.elb.us-east-1.amazonaws.com -->
# NextGenDatabridge Platform

A production-grade, modern **ELT pipeline platform** built on Apache Airflow, DuckDB, FastAPI, and React. Orchestrate complex data pipelines from SQL Server, Oracle, Kafka, Pub/Sub, and file sources to any target — with full audit trails, data quality checks, deployment approvals, and a live admin UI.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                   NextGenDatabridge Platform                      │
├──────────────┬──────────────────────┬────────────────────────────┤
│  Admin UI    │   FastAPI Backend    │   Apache Airflow           │
│  (React)     │   (REST + WebSocket) │   (CeleryExecutor)         │
│  :3000       │   :8000              │   :8080                    │
├──────────────┴──────────────────────┴────────────────────────────┤
│              PostgreSQL (Audit DB + Airflow DB)                  │
│              Redis (Celery broker)                               │
│              S3 / LocalStack (DuckDB store + configs)            │
├──────────────────────────────────────────────────────────────────┤
│  Pipeline Execution: Airflow DAGs → Custom Operators             │
│  Heavy Tasks: EKS Kubernetes Jobs (transform_job.py)             │
│  Monitoring: Prometheus + Grafana                                │
└──────────────────────────────────────────────────────────────────┘
```

### Supported Sources
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

### Supported Targets
| Type | Details |
|------|---------|
| SQL Server | Bulk-insert via SQLAlchemy |
| Oracle | Bulk-insert via cx_Oracle |
| PostgreSQL | COPY-style via SQLAlchemy |
| S3 / Parquet | pyarrow |
| Kafka | kafka-python producer |
| GCP Pub/Sub | google-cloud-pubsub publisher |

---

## Quick Start (Docker Compose)

### Prerequisites
- Docker Desktop ≥ 4.20 (16 GB RAM recommended)
- Docker Compose v2

### 1. Clone and start

```bash
git clone https://github.com/your-org/nextgen-databridge-platform.git
cd nextgen-databridge-platform

# Create .env file
cat > .env <<EOF
AIRFLOW_UID=50000
AWS_ACCESS_KEY_ID=localstack
AWS_SECRET_ACCESS_KEY=localstack
AWS_DEFAULT_REGION=us-east-1
EOF

# Start everything
docker compose up -d

# Watch logs
docker compose logs -f dataflow-api airflow-scheduler
```

### 2. Wait for services (~3 minutes first run)

```bash
# Check health
curl http://localhost:8000/health
curl http://localhost:8080/health  # Airflow
```

### 3. Access the platform

| Service | URL | Credentials |
|---------|-----|-------------|
| **DataFlow UI** | http://localhost:3000 | admin / admin |
| **DataFlow API** | http://localhost:8000/api/docs | — |
| **Airflow UI** | http://localhost:8080 | admin / admin |
| **Grafana** | http://localhost:3001 | admin / admin |
| **MailHog** | http://localhost:8025 | — |
| **Prometheus** | http://localhost:9090 | — |

---

## Project Structure

```
dataflow-platform/
├── airflow/
│   ├── dags/
│   │   └── dag_generator.py          # Dynamic DAG generator (reads all active configs)
│   └── plugins/
│       ├── dataflow_operators.py     # All custom Airflow operators
│       ├── dataflow_callbacks.py     # on_failure/on_success/SLA callbacks
│       └── config_loader.py         # Config loader (DB + S3 fallback)
├── backend/
│   ├── api/
│   │   └── main.py                  # FastAPI app (all routes, WebSocket, auth)
│   ├── models/
│   │   ├── models.py                # SQLAlchemy ORM models
│   │   └── database.py              # Async engine and session management
│   ├── services/
│   │   ├── audit_service.py         # Centralized audit logger
│   │   └── config_validator.py      # Pipeline JSON config validator
│   ├── migrations/                  # Alembic DB migrations
│   ├── Dockerfile
│   └── requirements.txt
├── frontend/
│   ├── src/
│   │   ├── api/client.ts            # Typed API client
│   │   ├── store/useStore.ts        # Zustand global state
│   │   ├── hooks/useWebSocket.ts    # Live event WebSocket hook
│   │   ├── components/
│   │   │   ├── Sidebar.tsx          # Navigation sidebar
│   │   │   └── ui.tsx               # Shared UI components
│   │   └── pages/
│   │       ├── Dashboard.tsx        # Main dashboard with metrics + charts
│   │       ├── Pipelines.tsx        # Pipeline registry + create
│   │       ├── PipelineDetail.tsx   # DAG view + config editor + run history
│   │       ├── Runs.tsx             # Run list + run detail + task rerun
│   │       ├── TasksAlertsAuditConnections.tsx
│   │       └── DeploymentsQueryEKS.tsx
│   ├── Dockerfile
│   └── nginx.conf
├── eks/
│   ├── jobs/
│   │   ├── transform_job.py         # EKS Job entrypoint for heavy transforms
│   │   └── Dockerfile
│   └── manifests/
│       └── platform.yaml            # K8s manifests (Namespace, SA, RBAC, Deployments)
├── infra/
│   ├── terraform/
│   │   ├── main.tf                  # EKS, RDS, ElastiCache, S3, ECR, IAM
│   │   └── variables.tf
│   ├── prometheus.yml               # Prometheus scrape config
│   └── grafana/
│       ├── datasources/
│       └── dashboards/              # Auto-provisioned DataFlow dashboard
├── scripts/
│   ├── init-multiple-dbs.sh         # Creates airflow + dataflow_audit DBs
│   └── init-localstack.sh           # Creates S3 buckets and SQS queues
├── tests/
│   ├── unit/
│   │   └── test_config_validator.py # 18 unit tests for config validation
│   └── integration/
│       └── test_api.py              # FastAPI integration tests
├── docs/
│   ├── sample_pipeline_orders_etl.json
│   └── sample_pipeline_kafka_clickstream.json
└── docker-compose.yml
```

---

## Pipeline Configuration

Pipelines are defined as JSON documents. The dynamic DAG generator picks up all active configs from the database (with S3 fallback) and creates Airflow DAGs automatically.

### Minimal example

```json
{
  "pipeline_id": "my_pipeline",
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
      "source": { "connection": "sqlserver_prod", "query": "SELECT * FROM orders WHERE updated_at >= '{{ ds }}'" },
      "output": { "duckdb_path": "s3://dataflow-duckdb-store/pipelines/my_pipeline/runs/{{ run_id }}/extract.duckdb", "table": "raw_orders" }
    },
    {
      "task_id": "transform",
      "type": "duckdb_transform",
      "depends_on": ["extract"],
      "sql": "SELECT order_id, SUM(amount) AS total FROM extract.raw_orders GROUP BY order_id",
      "output": { "duckdb_path": "s3://dataflow-duckdb-store/pipelines/my_pipeline/runs/{{ run_id }}/transform.duckdb", "table": "summary" }
    },
    {
      "task_id": "quality_check",
      "type": "data_quality",
      "depends_on": ["transform"],
      "checks": [
        { "type": "not_null", "column": "order_id", "action": "fail" },
        { "type": "row_count_min", "value": 1, "action": "fail" }
      ]
    },
    {
      "task_id": "load",
      "type": "load_target",
      "depends_on": ["quality_check"],
      "source": { "table": "summary" },
      "target": { "type": "s3", "bucket": "my-data-lake", "path": "orders/{{ ds }}/summary.parquet" }
    }
  ]
}
```

### Task types reference

| Type | Description |
|------|-------------|
| `sql_extract` | Extract from SQL Server, Oracle, PostgreSQL via SQL query |
| `duckdb_transform` | SQL transform using DuckDB (can JOIN multiple upstream DuckDB files) |
| `data_quality` | Run configurable QC checks (not_null, unique, row_count, freshness, regex, custom_sql) |
| `schema_validate` | Validate column names and types against expected schema |
| `file_ingest` | Ingest CSV/TSV/Parquet/JSON/Excel from S3 or local path |
| `kafka_consume` | Consume messages from Kafka topic into DuckDB |
| `kafka_produce` | Publish DuckDB data to Kafka topic |
| `pubsub_consume` | Pull messages from GCP Pub/Sub subscription |
| `pubsub_publish` | Publish to GCP Pub/Sub topic |
| `eks_job` | Submit heavy transform as a Kubernetes Job on EKS |
| `load_target` | Load DuckDB data to SQL Server, Oracle, PostgreSQL, S3, Kafka, or Pub/Sub |
| `conditional_branch` | Branch pipeline based on evaluated expression |
| `notification` | Send Slack / email notifications |
| `cdc_extract` | Incremental extract using change tracking |

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

## Audit System

Every event in the platform is recorded in the `audit_logs` table:

- Pipeline created/updated/paused/resumed
- Run started/completed/failed/cancelled
- Task started/completed/failed/retried/skipped
- Config versions updated
- Connection created/tested
- Deployment submitted/approved/rejected
- Query Explorer queries executed
- User logins

Every `task_runs` record captures:
- Input sources (connection, query, path)
- Output DuckDB S3 path, table name, row count, schema
- QC results (per-check pass/fail detail)
- EKS Job name and pod name
- Duration, attempts, error traceback
- Worker hostname

---

## Deployment Approval Workflow

1. Submit deployment via UI or API — specify pipeline, version, environment, approver email
2. Approver receives HTML email with **Approve** and **Reject** links (tokens expire in 24h)
3. Click Approve → deployment executes:
   - Config uploaded to S3
   - Airflow DAG unpaused
   - Pipeline version updated in registry
4. All steps logged to `deployments.deployment_log`

---

## Running Tests

```bash
# Unit tests (no external deps required)
cd dataflow-platform
pip install -r tests/requirements-test.txt -r backend/requirements.txt
pytest tests/unit/ -v

# Integration tests (requires running stack)
docker compose up -d
pytest tests/integration/ -v
```

---

## Production Deployment

### EKS (Kubernetes)

```bash
# Apply K8s manifests
kubectl apply -f eks/manifests/platform.yaml

# Provision infrastructure with Terraform
cd infra/terraform
terraform init
terraform plan -var="environment=production" -var="db_password=your-secure-password"
terraform apply
```

### CI/CD (GitHub Actions example)

```yaml
- name: Build and push images
  run: |
    aws ecr get-login-password | docker login --username AWS --password-stdin $ECR_URL
    docker build -t $ECR_URL/dataflow/api:$SHA ./backend
    docker push $ECR_URL/dataflow/api:$SHA
    docker build -t $ECR_URL/dataflow/transform:$SHA ./eks/jobs
    docker push $ECR_URL/dataflow/transform:$SHA

- name: Deploy to EKS
  run: |
    kubectl set image deployment/dataflow-api api=$ECR_URL/dataflow/api:$SHA -n dataflow
```

---

## Environment Variables

### Backend / Airflow

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL async URL | `postgresql+asyncpg://dataflow:dataflow@postgres:5432/dataflow_audit` |
| `AIRFLOW_URL` | Airflow webserver URL | `http://airflow-webserver:8080` |
| `DATAFLOW_AUDIT_DB_URL` | Audit DB (sync for operators) | — |
| `DATAFLOW_PIPELINE_CONFIGS_BUCKET` | S3 bucket for pipeline configs | `s3://dataflow-pipeline-configs` |
| `DATAFLOW_DUCKDB_BUCKET` | S3 bucket for DuckDB files | `dataflow-duckdb-store` |
| `SECRET_KEY` | JWT signing secret | — |
| `AWS_ENDPOINT_URL` | LocalStack endpoint (dev) | — |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook | — |
| `SMTP_HOST` | SMTP server | `mailhog` |
| `EKS_CLUSTER_NAME` | EKS cluster name | `dataflow-eks` |
| `GCP_PROJECT_ID` | GCP project for Pub/Sub | — |

---

## Adding a New Connection

1. Go to **Connections** in the UI
2. Click **New Connection**, fill in host/port/credentials
3. Click **Test** to verify connectivity
4. Use the `connection_id` in pipeline task configs

Connections are registered in both the DataFlow DB and Airflow's connection store.

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Orchestration | Apache Airflow 2.8.1 (CeleryExecutor) |
| Transform engine | DuckDB 0.10 |
| Intermediate storage | S3 (LocalStack for dev) |
| API | FastAPI + SQLAlchemy async |
| Auth | JWT (RS256 in prod, HS256 in dev) |
| Database | PostgreSQL 15 |
| Task queue | Celery + Redis |
| Frontend | React 18 + Vite + Tailwind CSS + Recharts |
| EKS Jobs | Kubernetes Jobs (boto3 + kubernetes SDK) |
| Monitoring | Prometheus + Grafana |
| Email (dev) | MailHog |
| Local cloud | LocalStack 3.0 |
| IaC | Terraform + Helm |
| Tests | pytest + pytest-asyncio |
