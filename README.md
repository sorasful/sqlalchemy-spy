# sqlalchemy-profiler

A small local profiler for SQLAlchemy queries. It uses the same event-based mechanism that OpenTelemetry's SQLAlchemy instrumentation (and tools like Logfire) use under the hood - `before_cursor_execute`, `after_cursor_execute`, `handle_error` - but outputs everything locally with no backend required.

Useful during development to see which queries run, how long they take, and which lines of code trigger them.

```
────────────────────────────────────────────────────────────────────────
  SQLAlchemy Profiler  -  6 queries  -  total 0.31ms
────────────────────────────────────────────────────────────────────────

  #    Duration   Op        SQL
  1      0.06ms  SELECT    SELECT users.id, users.name FROM users
  2      0.04ms  SELECT    SELECT posts.id, posts.title FROM posts WHERE pos...
  3      0.03ms  SELECT    SELECT posts.id, posts.title FROM posts WHERE pos...
  4      0.03ms  SELECT    SELECT posts.id, posts.title FROM posts WHERE pos...
  5      0.02ms  SELECT    SELECT posts.id, posts.title FROM posts WHERE pos...
  6      0.02ms  SELECT    SELECT posts.id, posts.title FROM posts WHERE pos...

  Top 3 slowest:
  1.     0.06ms  SELECT users.id, users.name FROM users
                ↳ app/routes/users.py:34 in list_users()
  2.     0.04ms  SELECT posts.id, posts.title FROM posts WHERE posts.user_id...
                ↳ app/routes/users.py:37 in list_users()

  Hot paths - most queries:
  1. 5×   0.14ms  app/routes/users.py:37 in list_users()
  2. 1×   0.06ms  app/routes/users.py:34 in list_users()

  Hot paths - most time:
  1.   0.14ms  (5×)  app/routes/users.py:37 in list_users()
  2.   0.06ms  (1×)  app/routes/users.py:34 in list_users()

────────────────────────────────────────────────────────────────────────
  Total: 0.31ms  |  Avg: 0.05ms  |  Slowest: 0.06ms
────────────────────────────────────────────────────────────────────────
```

The hot paths sections are handy for spotting N+1 patterns: seeing `routes/users.py:37` fire 5 times in a loop is usually enough to identify the problem.

## Install

```bash
uv add sqlalchemy-profiler
# or
pip install sqlalchemy-profiler
```

## Usage

### Context manager

```python
from sqlalchemy_profiler import Profiler

with Profiler() as prof:
    with Session(engine) as session:
        session.execute(text("SELECT * FROM users"))

prof.print_stats()
```

Passing no engine listens on the `Engine` class itself, so all engines in the process are covered. Pass one to scope it to a specific engine:

```python
with Profiler(engine) as prof:
    ...
```

`AsyncEngine` is accepted directly:

```python
async_engine = create_async_engine("postgresql+asyncpg://...")

with Profiler(async_engine) as prof:
    async with AsyncSession(async_engine) as session:
        await session.execute(...)
```

### Decorator

Works on sync and async functions:

```python
from sqlalchemy_profiler import profile

@profile()
def load_dashboard():
    ...

@profile()
async def load_dashboard():
    ...
```

### Raw data

```python
with Profiler() as prof:
    run_queries()

for q in prof.queries:
    print(q.statement, q.duration_ms, q.started_at, q.error)

print(f"{prof.query_count} queries in {prof.total_time_ms:.2f}ms")
```

Each `QueryRecord` has:

| Field | Type | Description |
|---|---|---|
| `statement` | `str` | SQL string |
| `params` | `tuple \| dict` | Bound parameters |
| `duration_ms` | `float` | Execution time in ms |
| `started_at` | `float` | Wall-clock start time (`time.time()`) |
| `operation` | `str` | First SQL keyword (`SELECT`, `INSERT`, …) |
| `error` | `str \| None` | Exception message if the query failed |
| `stack` | `list[FrameSummary]` | Filtered call stack |

### FastAPI middleware

```python
from sqlalchemy_profiler import Profiler
from starlette.middleware.base import BaseHTTPMiddleware

class SQLAlchemyProfilerMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        with Profiler() as prof:
            response = await call_next(request)
        print(f"[{request.method} {request.url.path}]")
        prof.print_stats()
        return response

app.add_middleware(SQLAlchemyProfilerMiddleware)
```

See [`examples/fastapi_app.py`](examples/fastapi_app.py) for a working example with response headers.

## Renderers

### Console

```python
from sqlalchemy_profiler import ConsoleRenderer

ConsoleRenderer(
    top_slow=5,          # N slowest queries (default: 5)
    show_stack=True,     # show call site under each slow query (default: True)
    show_callsites=True, # show hot path sections (default: True)
    top_callsites=5,     # N call sites to show per section (default: 5)
).render(prof)

# shortcut on the profiler
prof.print_stats(top_slow=10, show_callsites=False)
```

### HTML

Produces a self-contained HTML file - CSS and JS are inlined, no external dependencies.

```python
from sqlalchemy_profiler import HtmlRenderer

HtmlRenderer().open(prof)                 # write to a temp file and open in the browser
HtmlRenderer().save(prof, "report.html")  # write to a specific path
HtmlRenderer().render(prof)               # return the HTML string
```

The page has a sortable query table (click any column header), per-row expansion showing the full SQL with keyword highlighting, bound parameters, and call stack, a filter bar by operation type, and the same hot paths sections as the console output.

### JSON

```python
from sqlalchemy_profiler import JsonRenderer

JsonRenderer().render(prof)              # → JSON string
JsonRenderer().print(prof)              # print to stdout
JsonRenderer(indent=None).render(prof)  # compact output
```

## How it works

SQLAlchemy exposes an [event system](https://docs.sqlalchemy.org/en/20/core/events.html). Three events are enough:

| Event | Purpose |
|---|---|
| `before_cursor_execute` | start timer, snapshot the call stack |
| `after_cursor_execute` | stop timer |
| `handle_error` | record the error, stop timer |

The call stack is filtered to remove SQLAlchemy's own frames (matched by the installed package path) and dynamic frames, leaving only user code. The last remaining frame is the line that triggered the query.

When no engine is passed, events are attached to the `Engine` class rather than an instance, which covers all engines automatically.

## Examples

```bash
uv run examples/basic.py
uv run examples/n_plus_one.py
uv run examples/orm_relationships.py
uv run examples/decorator.py
uv run --with uvicorn uvicorn examples.fastapi_app:app --reload
```
