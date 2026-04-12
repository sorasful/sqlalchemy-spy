from __future__ import annotations

import inspect
import time
import traceback
from dataclasses import dataclass, field
from functools import wraps
from pathlib import Path
from typing import Any, cast

import sqlalchemy
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine

from sqlalchemy_profiler.renderers.base import BaseRender

# Path to the installed SQLAlchemy package - used to filter out internal frames
_SA_PATH = str(Path(sqlalchemy.__file__).parent)


@dataclass
class QueryRecord:
    statement: str
    params: tuple
    start_time: float
    end_time: float = 0.0
    error: str | None = None
    stack: list[traceback.FrameSummary] = field(default_factory=list)
    started_at: float = field(default_factory=time.time)

    @property
    def duration_ms(self) -> float:
        return (self.end_time - self.start_time) * 1000

    @property
    def operation(self) -> str:
        """First keyword of the SQL statement (SELECT, INSERT, …)."""
        return (
            self.statement.strip().split()[0].upper() if self.statement.strip() else "?"
        )


def _to_sync_engine(engine: Engine | AsyncEngine | None) -> Engine | type[Engine]:
    """Accept a sync Engine, an AsyncEngine, or None (→ Engine class for global listening)."""
    if engine is None:
        return Engine

    # AsyncEngine exposes .sync_engine; avoid a hard import of the async module
    sync = getattr(engine, "sync_engine", None)
    if sync is not None:
        return cast(Engine, sync)
    return cast(Engine, engine)


class Profiler:
    def __init__(
        self,
        engine: Any = None,
        *,
        capture_stack: bool = True,
        stack_depth: int = 8,
    ):
        self.engine = _to_sync_engine(engine)
        self.capture_stack = capture_stack
        self.stack_depth = stack_depth
        self.queries: list[QueryRecord] = []
        self._active = False

    def start(self) -> "Profiler":
        if self._active:
            return self
        self.queries.clear()
        event.listen(
            self.engine, "before_cursor_execute", self._before_execute, retval=True
        )
        event.listen(self.engine, "after_cursor_execute", self._after_execute)
        event.listen(self.engine, "handle_error", self._on_error)
        self._active = True
        return self

    def stop(self) -> "Profiler":
        if not self._active:
            return self
        event.remove(self.engine, "before_cursor_execute", self._before_execute)
        event.remove(self.engine, "after_cursor_execute", self._after_execute)
        event.remove(self.engine, "handle_error", self._on_error)
        self._active = False
        return self

    def reset(self) -> "Profiler":
        self.queries.clear()
        return self

    def __enter__(self) -> "Profiler":
        return self.start()

    def __exit__(self, *_) -> None:
        self.stop()

    def _before_execute(self, conn, cursor, statement, params, context, executemany):
        stack: list[traceback.FrameSummary] = []
        if self.capture_stack:
            stack = [
                f
                for f in traceback.extract_stack()
                if not f.filename.startswith(_SA_PATH)  # installed SQLAlchemy internals
                and not f.filename.startswith(
                    "<"
                )  # dynamic frames (<string>, <frozen …>)
                and "sqlalchemy_profiler" not in f.filename
            ][-self.stack_depth :]

        record = QueryRecord(
            statement=statement,
            params=params,
            start_time=time.perf_counter(),
            stack=stack,
        )
        context._profiler_record = record
        self.queries.append(record)
        return statement, params

    def _after_execute(self, conn, cursor, statement, params, context, executemany):
        record = getattr(context, "_profiler_record", None)
        if record is not None:
            record.end_time = time.perf_counter()

    def _on_error(self, context):
        record = getattr(context.execution_context, "_profiler_record", None)
        if record is not None:
            record.end_time = time.perf_counter()
            record.error = str(context.original_exception)

    @property
    def total_time_ms(self) -> float:
        return sum(q.duration_ms for q in self.queries)

    @property
    def query_count(self) -> int:
        return len(self.queries)

    def print_stats(
        self,
        renderer: type[BaseRender] | None = None,
        **kwargs: Any,
    ) -> None:
        """Print a console report. Shortcut for ConsoleRenderer().render(profiler)."""
        from sqlalchemy_profiler import ConsoleRenderer

        renderer = renderer if renderer is not None else ConsoleRenderer
        renderer(**kwargs).render(self)


def profile(engine: Engine | None = None, **kwargs: Any):
    """Decorator that profiles all queries executed inside a function.

    Works with both regular and async functions.

    Usage::

        @profile()
        def my_view(): ...

        @profile(engine)
        async def my_handler(): ...
    """
    profiler = Profiler(engine)

    def decorator(func):
        if inspect.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args, **kw):
                with profiler:
                    result = await func(*args, **kw)
                profiler.print_stats(**kwargs)
                return result

            return async_wrapper

        @wraps(func)
        def wrapper(*args, **kw):
            with profiler:
                result = func(*args, **kw)
            profiler.print_stats(**kwargs)
            return result

        return wrapper

    return decorator
