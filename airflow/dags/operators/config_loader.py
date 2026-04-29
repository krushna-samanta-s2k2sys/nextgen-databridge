"""
Config Loader
Loads the environment config first (connections, buckets, registries), then loads
active pipeline configs and resolves all symbolic references so operators receive
fully-qualified values without any environment-specific details baked into the
pipeline JSON files.

Environment config lives at:
  s3://<NEXTGEN_DATABRIDGE_PIPELINE_CONFIGS_BUCKET>/environments/<NEXTGEN_DATABRIDGE_ENV>.json

Pipeline configs live at:
  s3://<NEXTGEN_DATABRIDGE_PIPELINE_CONFIGS_BUCKET>/active/<pipeline_id>.json
  OR are read from the PostgreSQL audit DB (primary source).
"""
from __future__ import annotations

import copy
import json
import logging
import os
import re
from typing import Dict, List, Optional

logger = logging.getLogger("nextgen_databridge.config_loader")

# ─────────────────────────────────────────────────────────────────────────────
# Environment config
# ─────────────────────────────────────────────────────────────────────────────

_env_config_cache: Optional[Dict] = None


def load_environment_config() -> Dict:
    """
    Load the environment config from S3.
    Result is cached for the lifetime of the Airflow worker process.
    """
    global _env_config_cache
    if _env_config_cache is not None:
        return _env_config_cache

    env_name = os.getenv("NEXTGEN_DATABRIDGE_ENV", "dev")
    bucket   = os.getenv("NEXTGEN_DATABRIDGE_PIPELINE_CONFIGS_BUCKET", "")
    key      = f"environments/{env_name}.json"

    if not bucket:
        logger.error("NEXTGEN_DATABRIDGE_PIPELINE_CONFIGS_BUCKET env var is not set")
        return {}

    try:
        import boto3
        kwargs = dict(region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
        endpoint = os.getenv("AWS_ENDPOINT_URL")
        if endpoint:
            kwargs["endpoint_url"] = endpoint

        s3   = boto3.client("s3", **kwargs)
        resp = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(resp["Body"].read())
        logger.info(f"Loaded env config: environment={data.get('environment')} "
                    f"connections={list(data.get('connections', {}).keys())}")
        _env_config_cache = data
        return data
    except Exception as e:
        logger.error(f"Failed to load environment config from s3://{bucket}/{key}: {e}")
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# Symbolic reference resolution
# ─────────────────────────────────────────────────────────────────────────────

def _build_substitution_map(env_config: Dict) -> Dict[str, str]:
    """
    Flatten the env config into a single substitution map so that any
    ${key} placeholder in a pipeline config value can be replaced.

    Supported placeholders:
      ${duckdb_store}            → env["s3_buckets"]["duckdb_store"]
      ${pipeline_configs}        → env["s3_buckets"]["pipeline_configs"]
      ${registry.transform}      → env["container_registries"]["transform"]
      ${secret.wwi_sqlserver}    → env["secrets"]["wwi_sqlserver"]
    """
    subs: Dict[str, str] = {}

    for k, v in env_config.get("s3_buckets", {}).items():
        subs[k] = str(v)

    for k, v in env_config.get("container_registries", {}).items():
        subs[f"registry.{k}"] = str(v)

    for k, v in env_config.get("secrets", {}).items():
        subs[f"secret.{k}"] = str(v)

    return subs


def _resolve_string(value: str, subs: Dict[str, str]) -> str:
    """Replace all ${key} placeholders in a string."""
    def replacer(match):
        key = match.group(1)
        if key in subs:
            return subs[key]
        logger.warning(f"Unresolved placeholder: ${{{key}}}")
        return match.group(0)  # leave as-is

    return re.sub(r"\$\{([^}]+)\}", replacer, value)


def _resolve_value(value, subs: Dict[str, str]):
    """Recursively resolve placeholders in any JSON value."""
    if isinstance(value, str):
        return _resolve_string(value, subs)
    if isinstance(value, dict):
        return {k: _resolve_value(v, subs) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_value(item, subs) for item in value]
    return value


def _resolve_connections(config: dict, env_config: Dict) -> None:
    """
    Walk every task and resolve "connection": "<logical_name>" to the
    Airflow connection ID registered in the env config, injecting host/port/
    database metadata for the operator fallback path.
    """
    connections = env_config.get("connections", {})
    if not connections:
        return

    for task in config.get("tasks", []):
        for section_key in ("source", "target"):
            section = task.get(section_key)
            if not isinstance(section, dict):
                continue

            conn_name = section.get("connection")
            if not conn_name or conn_name not in connections:
                continue

            conn_def = connections[conn_name]
            # Airflow conn ID (resolved via Secrets Manager backend on MWAA)
            section["connection"] = conn_def.get("airflow_conn_id", conn_name)
            # Inject type / host / port / database for the env-fallback path
            # in _get_connection_from_env() — operators prefer BaseHook first.
            section.setdefault("connection_type", conn_def.get("type", "mssql"))
            section.setdefault("_host",     conn_def.get("host"))
            section.setdefault("_port",     conn_def.get("port", 1433))
            section.setdefault("_database", conn_def.get("database"))


def resolve_pipeline_config(pipeline_config: dict, env_config: Dict) -> dict:
    """
    Return a deep copy of pipeline_config with all symbolic references resolved.
    """
    config = copy.deepcopy(pipeline_config)
    subs   = _build_substitution_map(env_config)

    # Resolve placeholders throughout the whole config tree
    resolved = _resolve_value(config, subs)

    # Resolve connection names (needs the full connections dict, not just strings)
    _resolve_connections(resolved, env_config)

    return resolved


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline config loading
# ─────────────────────────────────────────────────────────────────────────────

def load_active_pipeline_configs() -> List[Dict]:
    """
    Load all active pipeline configurations, resolving env-specific references.
    """
    env_config = load_environment_config()

    raw_configs: List[Dict] = []

    # Primary: PostgreSQL audit DB
    try:
        raw_configs = _load_from_db()
    except Exception as e:
        logger.warning(f"DB config load failed, falling back to S3: {e}")

    # Fallback: S3 active/ prefix
    if not raw_configs:
        try:
            raw_configs = _load_from_s3()
        except Exception as e:
            logger.error(f"S3 config load also failed: {e}")

    return [resolve_pipeline_config(c, env_config) for c in raw_configs]


def load_pipeline_config(pipeline_id: str, version: Optional[str] = None) -> Optional[Dict]:
    """Load and resolve a single pipeline config by ID."""
    env_config = load_environment_config()
    raw        = _load_single_from_db(pipeline_id, version)
    if raw is None:
        raw = _load_single_from_s3(pipeline_id)
    return resolve_pipeline_config(raw, env_config) if raw else None


# ─────────────────────────────────────────────────────────────────────────────
# Raw loaders (no resolution)
# ─────────────────────────────────────────────────────────────────────────────

def _load_from_db() -> List[Dict]:
    import psycopg2
    url = os.environ["NEXTGEN_DATABRIDGE_AUDIT_DB_URL"].replace("postgresql+asyncpg://", "postgresql://")

    conn = psycopg2.connect(url)
    cur  = conn.cursor()
    cur.execute("""
        SELECT pc.config
        FROM pipeline_configs pc
        INNER JOIN pipelines p ON pc.pipeline_id = p.pipeline_id
        WHERE pc.is_active = true
          AND p.status = 'active'
        ORDER BY pc.pipeline_id
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    configs = [row[0] for row in rows]
    logger.info(f"Loaded {len(configs)} pipeline configs from DB")
    return configs


def _load_from_s3() -> List[Dict]:
    import boto3
    bucket = os.environ["NEXTGEN_DATABRIDGE_PIPELINE_CONFIGS_BUCKET"]
    kwargs = dict(region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    if endpoint:
        kwargs["endpoint_url"] = endpoint

    s3        = boto3.client("s3", **kwargs)
    paginator = s3.get_paginator("list_objects_v2")
    configs   = []

    for page in paginator.paginate(Bucket=bucket, Prefix="active/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                response = s3.get_object(Bucket=bucket, Key=obj["Key"])
                configs.append(json.loads(response["Body"].read()))

    logger.info(f"Loaded {len(configs)} pipeline configs from S3")
    return configs


def _load_single_from_db(pipeline_id: str, version: Optional[str]) -> Optional[Dict]:
    try:
        import psycopg2
        url = os.environ["NEXTGEN_DATABRIDGE_AUDIT_DB_URL"].replace("postgresql+asyncpg://", "postgresql://")
        conn = psycopg2.connect(url)
        cur  = conn.cursor()
        if version:
            cur.execute(
                "SELECT config FROM pipeline_configs WHERE pipeline_id = %s AND version = %s",
                (pipeline_id, version),
            )
        else:
            cur.execute(
                "SELECT config FROM pipeline_configs WHERE pipeline_id = %s AND is_active = true",
                (pipeline_id,),
            )
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row[0] if row else None
    except Exception as e:
        logger.error(f"DB load failed for {pipeline_id}: {e}")
        return None


def _load_single_from_s3(pipeline_id: str) -> Optional[Dict]:
    try:
        import boto3
        bucket = os.environ["NEXTGEN_DATABRIDGE_PIPELINE_CONFIGS_BUCKET"]
        kwargs = dict(region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
        endpoint = os.getenv("AWS_ENDPOINT_URL")
        if endpoint:
            kwargs["endpoint_url"] = endpoint
        s3   = boto3.client("s3", **kwargs)
        resp = s3.get_object(Bucket=bucket, Key=f"active/{pipeline_id}.json")
        return json.loads(resp["Body"].read())
    except Exception as e:
        logger.error(f"S3 load failed for {pipeline_id}: {e}")
        return None
