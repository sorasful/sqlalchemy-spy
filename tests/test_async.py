import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from sqlalchemy_spy import Profiler


@pytest.fixture
async def async_engine():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.execute(
            text("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        )
        await conn.execute(text("INSERT INTO items VALUES (1, 'foo'), (2, 'bar')"))
    yield engine
    await engine.dispose()


class TestAsyncProfiling:
    async def test_no_engine_captures_async_queries(self, async_engine):
        """Profiler() with no engine should capture async queries automatically."""
        with Profiler() as prof:
            async with AsyncSession(async_engine) as session:
                await session.execute(text("SELECT * FROM items"))
        assert prof.query_count == 1

    async def test_no_engine_captures_multiple_async_queries(self, async_engine):
        with Profiler() as prof:
            async with AsyncSession(async_engine) as session:
                await session.execute(text("SELECT * FROM items"))
                await session.execute(text("SELECT count(*) FROM items"))
        assert prof.query_count == 2

    async def test_sync_engine_arg_captures_async_queries(self, async_engine):
        """Passing async_engine.sync_engine explicitly should work."""
        with Profiler(async_engine.sync_engine) as prof:
            async with AsyncSession(async_engine) as session:
                await session.execute(text("SELECT * FROM items"))
        assert prof.query_count == 1

    async def test_async_engine_arg_captures_queries(self, async_engine):
        """Passing an AsyncEngine directly should work (auto-extracts sync_engine)."""
        with Profiler(async_engine) as prof:
            async with AsyncSession(async_engine) as session:
                await session.execute(text("SELECT * FROM items"))
        assert prof.query_count == 1

    async def test_records_duration(self, async_engine):
        with Profiler() as prof:
            async with AsyncSession(async_engine) as session:
                await session.execute(text("SELECT * FROM items"))
        assert prof.queries[0].duration_ms > 0

    async def test_does_not_capture_outside_context(self, async_engine):
        prof = Profiler()
        async with AsyncSession(async_engine) as session:
            await session.execute(text("SELECT 1"))
        assert prof.query_count == 0

    async def test_captures_error(self, async_engine):
        prof = Profiler()
        prof.start()
        try:
            async with AsyncSession(async_engine) as session:
                await session.execute(text("SELECT * FROM nonexistent"))
        except Exception:
            pass
        prof.stop()
        assert prof.query_count == 1
        assert prof.queries[0].error is not None
