import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from sqlalchemy_spy import Profiler


@pytest.fixture
def profiler_with_queries(engine):
    with Profiler(engine) as prof:
        with Session(engine) as s:
            s.execute(text("SELECT * FROM items"))
            s.execute(text("SELECT count(*) FROM items"))
    return prof


@pytest.fixture
def profiler_single_query(engine):
    with Profiler(engine) as prof:
        with Session(engine) as s:
            s.execute(text("SELECT * FROM items"))
    return prof


@pytest.fixture
def empty_profiler(engine):
    with Profiler(engine) as prof:
        pass
    return prof


@pytest.fixture
def profiler_with_error(engine):
    prof = Profiler(engine)
    prof.start()
    try:
        with Session(engine) as s:
            s.execute(text("SELECT * FROM nonexistent_table"))
    except Exception:
        pass
    prof.stop()
    return prof


@pytest.fixture
def profiler_no_stack(engine):
    with Profiler(engine, capture_stack=False) as prof:
        with Session(engine) as s:
            s.execute(text("SELECT * FROM items"))
    return prof
