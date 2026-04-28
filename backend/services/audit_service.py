"""
Audit Service
Centralized, immutable audit logging for all platform events.
Every pipeline, task, config, deploy, and user action goes through here.
"""
from __future__ import annotations
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc

from models.models import AuditLog, AuditEventType, AlertSeverity
from models.database import AsyncSessionLocal

logger = logging.getLogger("nextgen_databridge.audit")


class AuditService:
    """
    Write-through audit logger.
    All writes are fire-and-forget async — never block the main flow.
    """

    async def log(
        self,
        event_type: AuditEventType,
        *,
        pipeline_id: Optional[str] = None,
        run_id: Optional[str] = None,
        task_id: Optional[str] = None,
        user: Optional[str] = "system",
        ip_address: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        old_value: Optional[Dict] = None,
        new_value: Optional[Dict] = None,
        severity: AlertSeverity = AlertSeverity.INFO,
        correlation_id: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> Optional[AuditLog]:
        """
        Log an audit event. If session is provided, uses it; otherwise opens own session.
        """
        entry = AuditLog(
            event_type=event_type,
            pipeline_id=pipeline_id,
            run_id=run_id,
            task_id=task_id,
            user=user,
            ip_address=ip_address,
            details=details or {},
            old_value=old_value,
            new_value=new_value,
            severity=severity,
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
        )

        if session:
            session.add(entry)
            return entry

        # Fire-and-forget with own session
        try:
            async with AsyncSessionLocal() as db:
                db.add(entry)
                await db.commit()
                return entry
        except Exception as e:
            logger.error(f"Audit log write failed: {e}", exc_info=True)
            return None

    # ── Convenience helpers ───────────────────────────────────────────────

    async def pipeline_created(self, pipeline_id: str, config: dict, user: str, session=None):
        return await self.log(
            AuditEventType.PIPELINE_CREATED,
            pipeline_id=pipeline_id,
            user=user,
            new_value=config,
            severity=AlertSeverity.INFO,
            session=session,
        )

    async def pipeline_updated(self, pipeline_id: str, old_config: dict, new_config: dict, user: str, session=None):
        return await self.log(
            AuditEventType.PIPELINE_UPDATED,
            pipeline_id=pipeline_id,
            user=user,
            old_value=old_config,
            new_value=new_config,
            severity=AlertSeverity.INFO,
            session=session,
        )

    async def run_started(self, pipeline_id: str, run_id: str, trigger_type: str, triggered_by: str, session=None):
        return await self.log(
            AuditEventType.RUN_STARTED,
            pipeline_id=pipeline_id,
            run_id=run_id,
            user=triggered_by,
            details={"trigger_type": trigger_type},
            severity=AlertSeverity.INFO,
            session=session,
        )

    async def run_completed(self, pipeline_id: str, run_id: str, duration_seconds: float, rows: int, session=None):
        return await self.log(
            AuditEventType.RUN_COMPLETED,
            pipeline_id=pipeline_id,
            run_id=run_id,
            details={"duration_seconds": duration_seconds, "total_rows": rows},
            severity=AlertSeverity.INFO,
            session=session,
        )

    async def run_failed(self, pipeline_id: str, run_id: str, error: str, session=None):
        return await self.log(
            AuditEventType.RUN_FAILED,
            pipeline_id=pipeline_id,
            run_id=run_id,
            details={"error": error},
            severity=AlertSeverity.ERROR,
            session=session,
        )

    async def task_started(
        self,
        pipeline_id: str,
        run_id: str,
        task_id: str,
        task_type: str,
        input_sources: list,
        session=None,
    ):
        return await self.log(
            AuditEventType.TASK_STARTED,
            pipeline_id=pipeline_id,
            run_id=run_id,
            task_id=task_id,
            details={
                "task_type": task_type,
                "input_sources": input_sources,
            },
            severity=AlertSeverity.INFO,
            session=session,
        )

    async def task_completed(
        self,
        pipeline_id: str,
        run_id: str,
        task_id: str,
        output_duckdb_path: str,
        row_count: int,
        duration_seconds: float,
        session=None,
    ):
        return await self.log(
            AuditEventType.TASK_COMPLETED,
            pipeline_id=pipeline_id,
            run_id=run_id,
            task_id=task_id,
            details={
                "output_duckdb_path": output_duckdb_path,
                "row_count": row_count,
                "duration_seconds": round(duration_seconds, 3),
            },
            severity=AlertSeverity.INFO,
            session=session,
        )

    async def task_failed(
        self,
        pipeline_id: str,
        run_id: str,
        task_id: str,
        error: str,
        attempt: int,
        session=None,
    ):
        return await self.log(
            AuditEventType.TASK_FAILED,
            pipeline_id=pipeline_id,
            run_id=run_id,
            task_id=task_id,
            details={"error": error[:2000], "attempt": attempt},
            severity=AlertSeverity.ERROR,
            session=session,
        )

    async def task_rerun_requested(
        self,
        pipeline_id: str,
        run_id: str,
        task_id: str,
        user: str,
        reason: str,
        mode: str,
        session=None,
    ):
        return await self.log(
            AuditEventType.TASK_RERUN_REQUESTED,
            pipeline_id=pipeline_id,
            run_id=run_id,
            task_id=task_id,
            user=user,
            details={"reason": reason, "mode": mode},
            severity=AlertSeverity.WARNING,
            session=session,
        )

    async def deployment_submitted(self, deployment_id: str, pipeline_id: str, version: str, user: str, session=None):
        return await self.log(
            AuditEventType.DEPLOYMENT_SUBMITTED,
            pipeline_id=pipeline_id,
            user=user,
            details={"deployment_id": deployment_id, "version": version},
            severity=AlertSeverity.INFO,
            session=session,
        )

    async def deployment_approved(self, deployment_id: str, pipeline_id: str, approved_by: str, session=None):
        return await self.log(
            AuditEventType.DEPLOYMENT_APPROVED,
            pipeline_id=pipeline_id,
            user=approved_by,
            details={"deployment_id": deployment_id},
            severity=AlertSeverity.INFO,
            session=session,
        )

    async def config_updated(self, pipeline_id: str, old_version: str, new_version: str, user: str, session=None):
        return await self.log(
            AuditEventType.CONFIG_UPDATED,
            pipeline_id=pipeline_id,
            user=user,
            details={"old_version": old_version, "new_version": new_version},
            severity=AlertSeverity.INFO,
            session=session,
        )

    async def query_executed(
        self,
        user: str,
        pipeline_id: str,
        run_id: str,
        task_id: str,
        duckdb_path: str,
        query_preview: str,
        rows_returned: int,
        session=None,
    ):
        return await self.log(
            AuditEventType.QUERY_EXECUTED,
            pipeline_id=pipeline_id,
            run_id=run_id,
            task_id=task_id,
            user=user,
            details={
                "duckdb_path": duckdb_path,
                "query_preview": query_preview[:500],
                "rows_returned": rows_returned,
            },
            severity=AlertSeverity.INFO,
            session=session,
        )

    # ── Query helpers ─────────────────────────────────────────────────────

    async def get_events(
        self,
        session: AsyncSession,
        pipeline_id: Optional[str] = None,
        run_id: Optional[str] = None,
        event_types: Optional[list] = None,
        limit: int = 100,
        offset: int = 0,
    ):
        q = select(AuditLog).order_by(desc(AuditLog.timestamp))
        if pipeline_id:
            q = q.where(AuditLog.pipeline_id == pipeline_id)
        if run_id:
            q = q.where(AuditLog.run_id == run_id)
        if event_types:
            q = q.where(AuditLog.event_type.in_(event_types))
        q = q.offset(offset).limit(limit)
        result = await session.execute(q)
        return result.scalars().all()


# Singleton
audit = AuditService()
