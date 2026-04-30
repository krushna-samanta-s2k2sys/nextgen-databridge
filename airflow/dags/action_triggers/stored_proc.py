"""
Stored Procedure Action Trigger
Executes stored procedures on SQL Server, Oracle, or PostgreSQL.
Supports IN/OUT/INOUT parameters and optional result set capture to DuckDB on S3.

Oracle connectivity:
  - Uses python-oracledb in thin mode (no Oracle Instant Client required).
  - Falls back to cx_Oracle if oracledb is not installed.
  - OUT parameters use cursor.var() bound via an anonymous PL/SQL block.
  - REF CURSOR output params are detected and used as the result set source.
"""
from __future__ import annotations

import logging
import os
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from airflow.utils.context import Context

from operators.base import NextGenDatabridgeBaseOperator

logger = logging.getLogger("nextgen_databridge.action_triggers.stored_proc")


class StoredProcOperator(NextGenDatabridgeBaseOperator):
    """
    task_config keys:
      connection         (str, required) — Airflow connection ID
      procedure          (str, required) — "schema.ProcName" or just "ProcName"
      parameters         (list[dict])    — [{name, value, type?, direction?}]
                                           direction: "in" (default) | "out" | "inout"
      capture_resultset  (bool)          — write first result set to DuckDB on S3; default false
      output             (dict)          — {duckdb_path: "...", table: "result"}
                                           required when capture_resultset=true

    XCom outputs:
      output_parameters  (dict) — OUT/INOUT parameter values after execution
      row_count          (int)  — rows in result set (0 when not captured)
      duckdb_path        (str)  — S3 path of DuckDB file (only when capture_resultset=true)
    """

    def execute(self, context: Context):
        cfg      = self.task_config
        run_id   = context["run_id"]
        start_ts = datetime.now(timezone.utc)
        self._write_task_run(run_id, "running", start_time=start_ts)

        try:
            procedure         = cfg["procedure"]
            params_cfg        = cfg.get("parameters") or []
            capture_resultset = cfg.get("capture_resultset", False)
            output_cfg        = cfg.get("output") or {}

            conn_type, db_conn = self._get_connection()
            cur = db_conn.cursor()

            in_params  = [p for p in params_cfg if p.get("direction", "in") == "in"]
            out_params = [p for p in params_cfg if p.get("direction") in ("out", "inout")]

            if conn_type == "mssql":
                rows, output_values = self._exec_mssql(cur, procedure, in_params, out_params,
                                                        capture_resultset)
            elif conn_type == "oracle":
                rows, output_values = self._exec_oracle(cur, procedure, in_params, out_params,
                                                         capture_resultset)
            else:
                rows, output_values = self._exec_postgresql(cur, procedure, in_params, out_params,
                                                             capture_resultset)

            db_conn.commit()
            cur.close()
            db_conn.close()

            row_count             = len(rows)
            output_duckdb_path: Optional[str] = None

            if capture_resultset and rows:
                output_duckdb_path = self.duckdb_s3_path(run_id, self.task_config["task_id"])
                local_path         = self.local_duckdb_path(run_id, self.task_config["task_id"])
                self.ensure_local_dir(local_path)
                self._write_duckdb(rows, output_cfg.get("table", "result"), local_path)
                self.upload_duckdb(local_path, output_duckdb_path)
                logger.info(f"Stored proc result set ({row_count} rows) → {output_duckdb_path}")

            context["ti"].xcom_push(key="output_parameters", value=output_values)
            context["ti"].xcom_push(key="row_count", value=row_count)
            if output_duckdb_path:
                context["ti"].xcom_push(key="duckdb_path", value=output_duckdb_path)

            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
            self._write_task_run(run_id, "success", start_time=start_ts,
                                 end_time=datetime.now(timezone.utc),
                                 duration_seconds=duration,
                                 output_duckdb_path=output_duckdb_path,
                                 output_row_count=row_count,
                                 metrics={"procedure": procedure,
                                          "output_parameters": output_values})
            return {"row_count": row_count, "output_parameters": output_values}

        except Exception as exc:
            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
            self._write_task_run(run_id, "failed", start_time=start_ts,
                                 end_time=datetime.now(timezone.utc),
                                 duration_seconds=duration,
                                 error_message=str(exc),
                                 error_traceback=traceback.format_exc())
            raise

    # ── Connection resolution ──────────────────────────────────────────────────

    def _get_connection(self) -> Tuple[str, Any]:
        conn_id = self.task_config["connection"]
        try:
            from airflow.hooks.base import BaseHook
            conn      = BaseHook.get_connection(conn_id)
            conn_type = (conn.conn_type or "").lower()
            host      = conn.host or ""
            port      = conn.port
            login     = conn.login or ""
            pwd       = conn.password or ""
            schema    = conn.schema or ""
        except Exception:
            import urllib.parse
            url_env   = os.getenv(f"CONN_{conn_id.upper()}", "")
            if not url_env:
                raise
            p         = urllib.parse.urlparse(url_env)
            conn_type = p.scheme.replace("+pyodbc", "").replace("+pymssql", "")
            host      = p.hostname or ""
            port      = p.port
            login     = urllib.parse.unquote(p.username or "")
            pwd       = urllib.parse.unquote(p.password or "")
            schema    = p.path.lstrip("/")

        ct = conn_type.lower()

        if "mssql" in ct or "sqlserver" in ct:
            import pyodbc
            dsn = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={host},{port or 1433};DATABASE={schema};"
                f"UID={login};PWD={pwd};Encrypt=yes;TrustServerCertificate=yes"
            )
            return "mssql", pyodbc.connect(dsn, autocommit=False)

        if "oracle" in ct:
            return "oracle", self._oracle_connect(host, port, login, pwd, schema)

        return "postgresql", psycopg2.connect(
            host=host, port=port or 5432, dbname=schema, user=login, password=pwd,
        )

    def _oracle_connect(self, host: str, port, login: str, pwd: str, schema: str):
        """
        Connect to Oracle using python-oracledb thin mode (no Instant Client needed).
        schema is treated as the service_name / SID in the DSN.
        Falls back to cx_Oracle when oracledb is not installed.
        """
        try:
            import oracledb
            return oracledb.connect(
                user=login, password=pwd,
                dsn=f"{host}:{port or 1521}/{schema}",
            )
        except ImportError:
            import cx_Oracle
            dsn = cx_Oracle.makedsn(host, port or 1521, service_name=schema)
            return cx_Oracle.connect(login, pwd, dsn)

    # ── SQL Server execution ───────────────────────────────────────────────────

    def _exec_mssql(
        self, cur, procedure: str,
        in_params: List[dict], out_params: List[dict], capture: bool,
    ) -> Tuple[list, Dict[str, Any]]:
        import pyodbc

        parts  = []
        values = []
        for p in in_params:
            parts.append(f"@{p['name']}=?")
            values.append(p.get("value"))

        out_markers = []
        for p in out_params:
            marker = pyodbc.OUTPUT(p.get("value"))
            parts.append(f"@{p['name']}=? OUTPUT")
            values.append(marker)
            out_markers.append(marker)

        sql = f"EXEC {procedure} {', '.join(parts)}" if parts else f"EXEC {procedure}"
        cur.execute(sql, values)

        rows = []
        if capture:
            try:
                rows = [list(r) for r in (cur.fetchall() or [])]
            except pyodbc.ProgrammingError:
                pass

        output_values: Dict[str, Any] = {}
        for p, marker in zip(out_params, out_markers):
            try:
                output_values[p["name"]] = marker.value
            except Exception:
                output_values[p["name"]] = None

        return rows, output_values

    # ── Oracle execution ───────────────────────────────────────────────────────

    def _exec_oracle(
        self, cur, procedure: str,
        in_params: List[dict], out_params: List[dict], capture: bool,
    ) -> Tuple[list, Dict[str, Any]]:
        """
        Executes an Oracle stored procedure via an anonymous PL/SQL block.
        OUT parameters are bound with cursor.var().
        If an OUT parameter has type "refcursor", it is treated as the result set.
        """
        try:
            import oracledb as odb
        except ImportError:
            import cx_Oracle as odb

        bind_names = []
        bind_vals  = {}
        out_vars: Dict[str, Any] = {}
        refcursor_param: Optional[str] = None

        for p in in_params:
            bname            = f"p_{p['name']}"
            bind_names.append(f":{bname}")
            bind_vals[bname] = p.get("value")

        for p in out_params:
            bname = f"p_{p['name']}"
            ptype = (p.get("type") or "").lower()
            if ptype == "refcursor":
                var = cur.var(odb.CURSOR)
                refcursor_param = p["name"]
            else:
                # Map common type names; default to STRING for unknown types
                _type_map = {
                    "int": odb.NUMBER, "number": odb.NUMBER, "float": odb.NUMBER,
                    "date": odb.DATE, "timestamp": odb.TIMESTAMP,
                    "clob": odb.CLOB, "blob": odb.BLOB,
                }
                var = cur.var(_type_map.get(ptype, odb.STRING))
            bind_names.append(f":{bname}")
            bind_vals[bname] = var
            out_vars[p["name"]] = var

        sql = f"BEGIN {procedure}({', '.join(bind_names)}); END;"
        cur.execute(sql, bind_vals)

        output_values: Dict[str, Any] = {}
        rows: list = []

        for name, var in out_vars.items():
            val = var.getvalue()
            if name == refcursor_param:
                # REF CURSOR — fetch rows from the nested cursor
                if capture and val is not None:
                    rows = [list(r) for r in val.fetchall()]
                output_values[name] = f"<REF CURSOR: {len(rows)} rows>"
            else:
                output_values[name] = val

        return rows, output_values

    # ── PostgreSQL execution ───────────────────────────────────────────────────

    def _exec_postgresql(
        self, cur, procedure: str,
        in_params: List[dict], out_params: List[dict], capture: bool,
    ) -> Tuple[list, Dict[str, Any]]:
        all_params   = in_params + out_params
        placeholders = ", ".join(["%s"] * len(all_params))
        values       = [p.get("value") for p in all_params]
        sql          = (f"CALL {procedure}({placeholders})"
                        if all_params else f"CALL {procedure}()")
        cur.execute(sql, values)

        rows: list = []
        if capture:
            try:
                rows = [list(r) for r in (cur.fetchall() or [])]
            except Exception:
                pass

        output_values: Dict[str, Any] = {}
        if out_params:
            try:
                out_row = cur.fetchone()
                if out_row:
                    for i, p in enumerate(out_params):
                        output_values[p["name"]] = out_row[i]
            except Exception:
                pass

        return rows, output_values

    # ── DuckDB file write ──────────────────────────────────────────────────────

    def _write_duckdb(self, rows: list, table_name: str, local_path: str):
        import duckdb
        con = duckdb.connect(local_path)
        if rows:
            width        = len(rows[0])
            placeholders = ", ".join(["?"] * width)
            row_tuples   = ", ".join(["(" + placeholders + ")"] * len(rows))
            flat_vals    = [v for row in rows for v in row]
            con.execute(
                f"CREATE TABLE IF NOT EXISTS {table_name} AS "
                f"SELECT * FROM (VALUES {row_tuples})",
                flat_vals,
            )
        else:
            con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (placeholder VARCHAR)")
        con.close()
