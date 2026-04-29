"""Database engine and session management"""
import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from models.models import Base

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://nextgen_databridge:nextgen_databridge@localhost:5432/nextgen_databridge_audit"
)
CONFIG_DB_URL = os.getenv(
    "CONFIG_DB_URL",
    "postgresql+asyncpg://nextgen_databridge:nextgen_databridge@localhost:5432/nextgen_databridge_config"
)

# pool_size=5 per replica (2 replicas = 10 total connections against RDS).
# connect_args timeout=10 prevents a blocked DB from hanging startup for 300s.
engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_size=5,
    max_overflow=5,
    pool_pre_ping=True,
    connect_args={"timeout": 10},
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def create_tables():
    """Create all tables"""
    async with engine.begin() as conn:
        await conn.run_sync(lambda c: Base.metadata.create_all(c, checkfirst=True))


async def drop_tables():
    """Drop all tables — dev only"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
