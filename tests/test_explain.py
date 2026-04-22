from __future__ import annotations

import warnings

from sqlalchemy import text
from sqlalchemy.orm import Session

from sqlalchemy_spy import Profiler
from sqlalchemy_spy.renderers.console import ConsoleRenderer
from sqlalchemy_spy.renderers.html import _fmt_explain
from sqlalchemy_spy.renderers.json import JsonRenderer


class TestExplainDisabled:
    def test_explain_false_by_default(self, engine):
        with Profiler(engine) as p:
            with Session(engine) as s:
                s.execute(text("SELECT * FROM items"))
        assert p.queries[0].explain_plan is None

    def test_explain_false_explicit(self, engine):
        with Profiler(engine, explain=False) as p:
            with Session(engine) as s:
                s.execute(text("SELECT * FROM items"))
        assert p.queries[0].explain_plan is None


class TestExplainEnabled:
    def test_explain_true_populates_plan(self, engine):
        with Profiler(engine, explain=True) as p:
            with Session(engine) as s:
                s.execute(text("SELECT * FROM items"))
        plan = p.queries[0].explain_plan
        assert plan is not None
        assert isinstance(plan, list)
        assert len(plan) > 0

    def test_explain_plan_is_string_lines(self, engine):
        with Profiler(engine, explain=True) as p:
            with Session(engine) as s:
                s.execute(text("SELECT * FROM items WHERE id = 1"))
        plan = p.queries[0].explain_plan
        assert plan is not None
        assert all(isinstance(line, str) for line in plan)

    def test_explain_skipped_for_pragma(self, engine):
        with Profiler(engine, explain=True) as p:
            with Session(engine) as s:
                s.execute(text("PRAGMA table_info(items)"))
        assert p.queries[0].explain_plan == []

    def test_explain_does_not_record_explain_query_itself(self, engine):
        with Profiler(engine, explain=True) as p:
            with Session(engine) as s:
                s.execute(text("SELECT * FROM items"))
        assert p.query_count == 1

    def test_explain_multiple_queries(self, engine):
        with Profiler(engine, explain=True) as p:
            with Session(engine) as s:
                s.execute(text("SELECT * FROM items"))
                s.execute(text("SELECT id FROM items WHERE id = 1"))
        assert p.query_count == 2
        assert p.queries[0].explain_plan is not None
        assert p.queries[1].explain_plan is not None


class TestConsoleRendererExplain:
    def test_console_renders_plan(self, engine, capsys):
        with Profiler(engine, explain=True) as p:
            with Session(engine) as s:
                s.execute(text("SELECT * FROM items"))
        ConsoleRenderer(top_slow=1).render(p)
        out = capsys.readouterr().out
        assert "SCAN" in out.upper() or "SEARCH" in out.upper() or "items" in out

    def test_console_no_plan_when_disabled(self, engine, capsys):
        with Profiler(engine, explain=False) as p:
            with Session(engine) as s:
                s.execute(text("SELECT * FROM items"))
        ConsoleRenderer(top_slow=1).render(p)
        out = capsys.readouterr().out
        assert "SCAN TABLE" not in out and "SEARCH TABLE" not in out


class TestJsonRendererExplain:
    def test_json_includes_explain_plan(self, engine):
        import json

        with Profiler(engine, explain=True) as p:
            with Session(engine) as s:
                s.execute(text("SELECT * FROM items"))
        data = json.loads(JsonRenderer().render(p))
        plan = data["queries"][0]["explain_plan"]
        assert plan is not None
        assert isinstance(plan, list)

    def test_json_explain_null_when_disabled(self, engine):
        import json

        with Profiler(engine, explain=False) as p:
            with Session(engine) as s:
                s.execute(text("SELECT * FROM items"))
        data = json.loads(JsonRenderer().render(p))
        assert data["queries"][0]["explain_plan"] is None


class TestExplainAsync:
    async def test_explain_works_with_aiosqlite(self):
        """explain=True must populate the plan when using an async driver (aiosqlite)."""
        from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with engine.begin() as conn:
            await conn.execute(
                text("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
            )
            await conn.execute(text("INSERT INTO t VALUES (1, 'a'), (2, 'b')"))

        with Profiler(engine, explain=True) as p:
            async with AsyncSession(engine) as s:
                await s.execute(text("SELECT * FROM t"))

        await engine.dispose()

        assert p.query_count == 1
        plan = p.queries[0].explain_plan
        assert plan is not None
        assert len(plan) > 0
        assert all(isinstance(line, str) for line in plan)

    async def test_explain_does_not_double_record_with_async(self):
        """The internal EXPLAIN query must not appear in the recorded query list."""
        from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with engine.begin() as conn:
            await conn.execute(text("CREATE TABLE t2 (id INTEGER PRIMARY KEY)"))

        with Profiler(engine, explain=True) as p:
            async with AsyncSession(engine) as s:
                await s.execute(text("SELECT * FROM t2"))

        await engine.dispose()
        assert p.query_count == 1


class TestHtmlFmtExplain:
    def test_fmt_explain_none_returns_empty(self):
        assert _fmt_explain(None) == ""

    def test_fmt_explain_empty_list_returns_empty(self):
        assert _fmt_explain([]) == ""

    def test_fmt_explain_full_scan_badge(self):
        result = _fmt_explain(["SCAN products"])
        assert "plan-badge full-scan" in result
        assert "Full Scan" in result

    def test_fmt_explain_index_badge(self):
        result = _fmt_explain(
            ["SEARCH products USING INDEX idx_products_category (category=?)"]
        )
        assert "plan-badge index" in result
        assert ">Index<" in result

    def test_fmt_explain_composite_index_badge(self):
        result = _fmt_explain(
            ["SEARCH products USING INDEX idx_cat_price (category=? AND price<?)"]
        )
        assert "plan-badge composite-index" in result
        assert "Composite Index" in result

    def test_fmt_explain_includes_label(self):
        result = _fmt_explain(["SCAN products"])
        assert "Execution plan" in result

    def test_fmt_explain_lines_collapsed_by_default(self):
        result = _fmt_explain(["SCAN products"])
        assert 'class="explain"' in result
        assert "display:none" not in result  # CSS handles it, not inline style

    def test_fmt_explain_has_toggle_button(self):
        result = _fmt_explain(["SCAN products"])
        assert "plan-tog" in result
        assert "togglePlan" in result

    def test_fmt_explain_escapes_html(self):
        result = _fmt_explain(["<script>alert(1)</script>"])
        assert "<script>" not in result
        assert "&lt;script&gt;" in result


class TestExplainUnsupportedDialect:
    def test_warns_on_unsupported_dialect(self, engine, monkeypatch):
        monkeypatch.setattr(engine.dialect, "name", "mysql")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with Profiler(engine, explain=True):
                with Session(engine) as s:
                    s.execute(text("SELECT * FROM items"))
        msgs = [str(w.message) for w in caught if issubclass(w.category, UserWarning)]
        assert any("mysql" in m and "not supported" in m for m in msgs)

    def test_no_warn_for_sqlite(self, engine):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with Profiler(engine, explain=True):
                with Session(engine) as s:
                    s.execute(text("SELECT * FROM items"))
        user_warns = [w for w in caught if issubclass(w.category, UserWarning)]
        assert not user_warns

    def test_no_warn_for_asyncpg_driver(self, engine, monkeypatch):
        # asyncpg is supported via SQLAlchemy's greenlet bridge — no warning expected
        monkeypatch.setattr(engine.dialect, "driver", "asyncpg")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with Profiler(engine, explain=True):
                with Session(engine) as s:
                    s.execute(text("SELECT * FROM items"))
        user_warns = [w for w in caught if issubclass(w.category, UserWarning)]
        assert not user_warns

    def test_warning_mentions_asyncpg_as_supported(self, engine, monkeypatch):
        monkeypatch.setattr(engine.dialect, "name", "mysql")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with Profiler(engine, explain=True):
                with Session(engine) as s:
                    s.execute(text("SELECT * FROM items"))
        msgs = [str(w.message) for w in caught if issubclass(w.category, UserWarning)]
        assert any("asyncpg" in m for m in msgs)


class TestHtmlRendererExplain:
    def test_html_includes_explain_section(self, engine):
        with Profiler(engine, explain=True) as p:
            with Session(engine) as s:
                s.execute(text("SELECT * FROM items"))
        from sqlalchemy_spy.renderers.html import HtmlRenderer

        html = HtmlRenderer().render(p)
        assert "Execution plan" in html
        assert "explain-row" in html

    def test_html_no_explain_when_disabled(self, engine):
        with Profiler(engine, explain=False) as p:
            with Session(engine) as s:
                s.execute(text("SELECT * FROM items"))
        from sqlalchemy_spy.renderers.html import HtmlRenderer

        html = HtmlRenderer().render(p)
        assert "Execution plan" not in html
