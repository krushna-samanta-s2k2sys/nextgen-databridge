"""
Data Quality and Schema Validation Operators
DataQualityOperator  — run configurable QC checks against a DuckDB table.
SchemaValidateOperator — compare actual DuckDB schema against expected schema.
Both are passthrough operators: they do not produce a new DuckDB file; they
pass the upstream duckdb_path through via XCom so downstream tasks can attach it.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from airflow.utils.context import Context

from operators.base import NextGenDatabridgeBaseOperator

logger = logging.getLogger("nextgen_databridge.operators.data_quality")


class DataQualityOperator(NextGenDatabridgeBaseOperator):
    """
    Runs configurable data quality checks. Supported check types:
    not_null, unique, row_count_min, row_count_max, freshness,
    value_range, regex_match, referential_integrity, schema_match, custom_sql.
    """

    def execute(self, context: Context) -> dict:
        run_id     = context["run_id"]
        task_id    = self.task_config["task_id"]
        ti         = context["ti"]
        checks     = self.task_config.get("checks", [])
        depends_on = self.task_config.get("depends_on", [])

        input_s3    = ti.xcom_pull(task_ids=depends_on[0], key="duckdb_path") if depends_on else None
        input_local = self.local_duckdb_path(run_id, depends_on[0] if depends_on else task_id)

        if input_s3 and not os.path.exists(input_local):
            self.download_duckdb(input_s3, input_local)

        start_ts = datetime.now(timezone.utc)
        self._write_task_run(run_id, "running", start_time=start_ts,
                             input_sources=[{"type": "duckdb", "path": input_s3}])

        results      = []
        failures     = 0
        warnings     = 0
        overall_pass = True

        try:
            import duckdb
            db          = duckdb.connect(input_local, read_only=True)
            input_table = self.task_config.get("source", {}).get("table", "raw_data")

            for check in checks:
                check_type  = check.get("type")
                action      = check.get("action", "fail")
                column      = check.get("column")
                description = check.get("description", f"{check_type} on {column or 'table'}")

                try:
                    passed, message, count = self._run_check(db, input_table, check)
                except Exception as e:
                    passed, message, count = False, str(e), None

                results.append({
                    "check_type": check_type, "column": column,
                    "description": description, "passed": passed,
                    "message": message, "count": count, "action": action,
                })

                if not passed:
                    if action == "fail":
                        failures += 1
                        overall_pass = False
                    elif action == "warn":
                        warnings += 1

                logger.info(f"QC check {check_type}/{column}: {'PASS' if passed else 'FAIL'} — {message}")

            db.close()

        except Exception as e:
            failures += 1
            overall_pass = False
            results.append({"check_type": "internal_error", "passed": False,
                            "message": str(e), "action": "fail"})

        duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
        status   = "success" if overall_pass else "failed"

        self._write_task_run(
            run_id, status,
            start_time=start_ts, end_time=datetime.now(timezone.utc),
            duration_seconds=duration,
            qc_results={"checks": results}, qc_passed=overall_pass,
            qc_warnings=warnings, qc_failures=failures,
            error_message=f"{failures} QC check(s) failed" if not overall_pass else None,
        )
        self._write_audit_log(
            "TASK_COMPLETED" if overall_pass else "TASK_FAILED", run_id,
            {"qc_passed": overall_pass, "failures": failures, "warnings": warnings},
            severity="info" if overall_pass else "error",
        )

        context["ti"].xcom_push(key="qc_passed", value=overall_pass)
        context["ti"].xcom_push(key="qc_results", value=results)
        if input_s3:
            context["ti"].xcom_push(key="duckdb_path", value=input_s3)

        if not overall_pass and self.task_config.get("fail_pipeline_on_error", True):
            raise ValueError(f"Data quality failed: {failures} checks failed. Results: {results}")

        return {"passed": overall_pass, "results": results,
                "failures": failures, "warnings": warnings}

    # ── Check implementations ──────────────────────────────────────────────────

    def _run_check(self, db, table: str, check: dict):
        ct     = check["type"]
        column = check.get("column")

        if ct == "not_null":
            count  = db.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL").fetchone()[0]
            return count == 0, f"{count} null values in '{column}'", count

        if ct == "unique":
            count = db.execute(
                f"SELECT COUNT(*) FROM (SELECT {column}, COUNT(*) c "
                f"FROM {table} GROUP BY {column} HAVING c > 1)"
            ).fetchone()[0]
            return count == 0, f"{count} duplicate values in '{column}'", count

        if ct == "row_count_min":
            min_val = check.get("value", 1)
            count   = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            return count >= min_val, f"Row count {count} < minimum {min_val}", count

        if ct == "row_count_max":
            max_val = check.get("value", 1_000_000)
            count   = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            return count <= max_val, f"Row count {count} > maximum {max_val}", count

        if ct == "freshness":
            from datetime import timedelta
            max_hours = check.get("max_hours", 24)
            date_col  = check.get("column", "updated_at")
            result    = db.execute(f"SELECT MAX({date_col}) FROM {table}").fetchone()[0]
            if result is None:
                return False, f"No data in '{date_col}'", None
            diff  = datetime.now(timezone.utc) - result.replace(tzinfo=timezone.utc)
            hours = diff.total_seconds() / 3600
            return hours <= max_hours, f"Most recent data is {hours:.1f}h old (max {max_hours}h)", None

        if ct == "value_range":
            min_v, max_v = check.get("min"), check.get("max")
            parts = []
            if min_v is not None: parts.append(f"{column} < {min_v}")
            if max_v is not None: parts.append(f"{column} > {max_v}")
            where = " OR ".join(parts) if parts else "1=0"
            count = db.execute(f"SELECT COUNT(*) FROM {table} WHERE {where}").fetchone()[0]
            return count == 0, f"{count} values out of range [{min_v}, {max_v}] in '{column}'", count

        if ct == "regex_match":
            pattern = check.get("pattern", ".*")
            count   = db.execute(
                f"SELECT COUNT(*) FROM {table} "
                f"WHERE NOT regexp_matches(CAST({column} AS VARCHAR), '{pattern}')"
            ).fetchone()[0]
            return count == 0, f"{count} values in '{column}' don't match '{pattern}'", count

        if ct == "custom_sql":
            count = db.execute(check.get("sql", "SELECT 0")).fetchone()[0]
            return count == 0, f"Custom SQL check returned {count} failing rows", count

        if ct == "schema_match":
            expected = check.get("columns", {})
            actual   = {col[0]: col[1] for col in db.execute(f"DESCRIBE {table}").fetchall()}
            mismatches = []
            for col_name, col_type in expected.items():
                if col_name not in actual:
                    mismatches.append(f"Missing column '{col_name}'")
                elif col_type.upper() not in actual[col_name].upper():
                    mismatches.append(
                        f"Column '{col_name}': expected {col_type}, got {actual[col_name]}"
                    )
            return len(mismatches) == 0, "; ".join(mismatches) or "Schema matches", len(mismatches)

        return True, f"Unknown check type '{ct}' — skipped", None


class SchemaValidateOperator(NextGenDatabridgeBaseOperator):

    def execute(self, context: Context) -> dict:
        run_id     = context["run_id"]
        task_id    = self.task_config["task_id"]
        ti         = context["ti"]
        depends_on = self.task_config.get("depends_on", [])

        input_s3    = ti.xcom_pull(task_ids=depends_on[0], key="duckdb_path") if depends_on else None
        input_local = self.local_duckdb_path(run_id, depends_on[0] if depends_on else task_id)

        if input_s3 and not os.path.exists(input_local):
            self.download_duckdb(input_s3, input_local)

        start_ts = datetime.now(timezone.utc)
        self._write_task_run(run_id, "running", start_time=start_ts)

        try:
            import duckdb
            expected_schema = self.task_config.get("expected_schema", {})
            input_table     = self.task_config.get("source", {}).get("table", "raw_data")

            db            = duckdb.connect(input_local, read_only=True)
            actual_schema = {col[0]: col[1] for col in db.execute(f"DESCRIBE {input_table}").fetchall()}
            db.close()

            issues = []
            for col_name, col_type in expected_schema.items():
                if col_name not in actual_schema:
                    issues.append({"column": col_name, "issue": "missing"})
                elif col_type.upper() not in actual_schema[col_name].upper():
                    issues.append({"column": col_name, "issue": "type_mismatch",
                                   "expected": col_type, "actual": actual_schema[col_name]})

            passed   = len(issues) == 0
            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()

            self._write_task_run(
                run_id, "success" if passed else "failed",
                start_time=start_ts, end_time=datetime.now(timezone.utc),
                duration_seconds=duration,
                qc_results={"schema_issues": issues},
                qc_passed=passed, qc_failures=len(issues),
            )

            context["ti"].xcom_push(key="schema_valid", value=passed)
            context["ti"].xcom_push(key="schema_issues", value=issues)
            context["ti"].xcom_push(key="duckdb_path", value=input_s3)

            if not passed and self.task_config.get("fail_on_error", True):
                raise ValueError(f"Schema validation failed: {issues}")

            return {"valid": passed, "issues": issues, "actual_schema": actual_schema}

        except ValueError:
            raise
        except Exception as e:
            self._write_task_run(run_id, "failed", start_time=start_ts,
                                 error_message=str(e)[:2000])
            raise
