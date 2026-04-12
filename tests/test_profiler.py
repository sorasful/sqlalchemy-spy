import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from sqlalchemy_profiler import Profiler
from sqlalchemy_profiler.profiler import QueryRecord


class TestQueryRecord:
    def test_duration_ms(self):
        record = QueryRecord(
            statement="SELECT 1", params=(), start_time=1.0, end_time=1.005
        )
        assert record.duration_ms == pytest.approx(5.0)

    def test_operation_extracts_first_keyword(self):
        assert (
            QueryRecord(statement="select * from t", params=(), start_time=0).operation
            == "SELECT"
        )
        assert (
            QueryRecord(
                statement="INSERT INTO t VALUES (1)", params=(), start_time=0
            ).operation
            == "INSERT"
        )
        assert (
            QueryRecord(
                statement="  \n  UPDATE t SET x=1", params=(), start_time=0
            ).operation
            == "UPDATE"
        )
        assert (
            QueryRecord(statement="DELETE FROM t", params=(), start_time=0).operation
            == "DELETE"
        )

    def test_operation_empty_statement(self):
        assert QueryRecord(statement="", params=(), start_time=0).operation == "?"
        assert QueryRecord(statement="   ", params=(), start_time=0).operation == "?"


class TestProfilerLifecycle:
    def test_start_registers_listeners(self, engine):
        from sqlalchemy import event

        prof = Profiler(engine)
        assert not prof._active
        prof.start()
        assert prof._active
        assert event.contains(engine, "before_cursor_execute", prof._before_execute)
        prof.stop()

    def test_stop_removes_listeners(self, engine):
        from sqlalchemy import event

        prof = Profiler(engine)
        prof.start()
        prof.stop()
        assert not prof._active
        assert not event.contains(engine, "before_cursor_execute", prof._before_execute)

    def test_start_is_idempotent(self, engine):
        prof = Profiler(engine)
        prof.start()
        prof.start()  # should not raise or double-register
        assert prof._active
        prof.stop()

    def test_stop_is_idempotent(self, engine):
        prof = Profiler(engine)
        prof.start()
        prof.stop()
        prof.stop()  # should not raise
        assert not prof._active

    def test_context_manager(self, engine, session):
        with Profiler(engine) as prof:
            session.execute(text("SELECT 1"))
        assert not prof._active
        assert prof.query_count == 1

    def test_reset_clears_queries(self, engine, session):
        with Profiler(engine) as prof:
            session.execute(text("SELECT 1"))
        assert prof.query_count == 1
        prof.reset()
        assert prof.query_count == 0


class TestQueryCapture:
    def test_captures_select(self, engine, session):
        with Profiler(engine) as prof:
            session.execute(text("SELECT * FROM items"))
        assert prof.query_count == 1
        assert prof.queries[0].operation == "SELECT"

    def test_captures_multiple_queries(self, engine, session):
        with Profiler(engine) as prof:
            session.execute(text("SELECT * FROM items"))
            session.execute(text("SELECT count(*) FROM items"))
            session.execute(text("SELECT name FROM items WHERE id = 1"))
        assert prof.query_count == 3

    def test_captures_statement_text(self, engine, session):
        with Profiler(engine) as prof:
            session.execute(text("SELECT name FROM items WHERE id = 1"))
        assert "SELECT" in prof.queries[0].statement
        assert "items" in prof.queries[0].statement

    def test_records_duration(self, engine, session):
        with Profiler(engine) as prof:
            session.execute(text("SELECT * FROM items"))
        assert prof.queries[0].duration_ms > 0

    def test_no_capture_outside_context(self, engine, session):
        prof = Profiler(engine)
        session.execute(text("SELECT 1"))
        assert prof.query_count == 0

    def test_does_not_capture_after_stop(self, engine, session):
        with Profiler(engine) as prof:
            session.execute(text("SELECT 1"))
        session.execute(text("SELECT 2"))  # outside - must not be captured
        assert prof.query_count == 1

    def test_total_time_ms(self, engine, session):
        with Profiler(engine) as prof:
            session.execute(text("SELECT 1"))
            session.execute(text("SELECT 2"))
        assert prof.total_time_ms == pytest.approx(
            sum(q.duration_ms for q in prof.queries)
        )

    def test_captures_error(self, engine):
        prof = Profiler(engine)
        prof.start()
        with Session(engine) as session:
            try:
                session.execute(text("SELECT * FROM nonexistent_table"))
            except Exception:
                pass
        prof.stop()
        assert prof.query_count == 1
        assert prof.queries[0].error is not None


class TestStackCapture:
    def test_captures_stack_by_default(self, engine, session):
        with Profiler(engine) as prof:
            session.execute(text("SELECT 1"))
        assert len(prof.queries[0].stack) > 0

    def test_stack_disabled(self, engine, session):
        with Profiler(engine, capture_stack=False) as prof:
            session.execute(text("SELECT 1"))
        assert prof.queries[0].stack == []

    def test_stack_points_to_caller(self, engine, session):
        with Profiler(engine) as prof:
            session.execute(text("SELECT 1"))
        filenames = [f.filename for f in prof.queries[0].stack]
        # At least one frame should be this test file
        assert any("test_profiler" in fn for fn in filenames)


class TestGlobalProfiling:
    def test_captures_all_engines_when_no_engine_given(self):
        engine_a = create_engine("sqlite:///:memory:")
        engine_b = create_engine("sqlite:///:memory:")
        try:
            with Profiler() as prof:
                with Session(engine_a) as s:
                    s.execute(text("SELECT 1"))
                with Session(engine_b) as s:
                    s.execute(text("SELECT 2"))
            assert prof.query_count == 2
        finally:
            engine_a.dispose()
            engine_b.dispose()
