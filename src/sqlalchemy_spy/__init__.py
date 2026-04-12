"""
sqlalchemy-spy - Minimal SQLAlchemy query profiler.

Usage::

    from sqlalchemy_spy import Profiler, profile

    with Profiler() as prof:
        session.execute(...)
    prof.print_stats()
"""

from sqlalchemy_spy.profiler import Profiler, QueryRecord, profile
from sqlalchemy_spy.renderers.console import ConsoleRenderer
from sqlalchemy_spy.renderers.html import HtmlRenderer
from sqlalchemy_spy.renderers.json import JsonRenderer

__all__ = [
    "Profiler",
    "QueryRecord",
    "profile",
    "ConsoleRenderer",
    "HtmlRenderer",
    "JsonRenderer",
]
