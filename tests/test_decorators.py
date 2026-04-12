from sqlalchemy import text
from sqlalchemy.orm import Session

from sqlalchemy_profiler import profile


class TestProfileDecorator:
    def test_sync_function_is_profiled(self, engine, capsys):
        @profile(engine)
        def run():
            with Session(engine) as s:
                s.execute(text("SELECT 1"))

        run()
        captured = capsys.readouterr()
        assert "1 query" in captured.out

    def test_sync_function_returns_value(self, engine):
        @profile(engine)
        def run():
            return 42

        assert run() == 42

    def test_multiple_calls_each_print(self, engine, capsys):
        @profile(engine)
        def run():
            with Session(engine) as s:
                s.execute(text("SELECT 1"))

        run()
        run()
        captured = capsys.readouterr()
        # Two separate stat blocks printed
        assert captured.out.count("SQLAlchemy Profiler") == 2

    async def test_async_function_is_profiled(self, engine, capsys):
        @profile(engine)
        async def run():
            with Session(engine) as s:
                s.execute(text("SELECT 1"))

        await run()
        captured = capsys.readouterr()
        assert "1 query" in captured.out

    async def test_async_function_returns_value(self, engine):
        @profile(engine)
        async def run():
            return "hello"

        result = await run()
        assert result == "hello"

    async def test_async_queries_captured_not_zero(self, engine, capsys):
        @profile(engine)
        async def run():
            with Session(engine) as s:
                s.execute(text("SELECT * FROM items"))
                s.execute(text("SELECT count(*) FROM items"))

        await run()
        captured = capsys.readouterr()
        assert "2 queries" in captured.out
