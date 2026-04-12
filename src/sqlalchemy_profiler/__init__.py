"""
sqlalchemy-profiler - Minimal SQLAlchemy query profiler.

Usage::

    from sqlalchemy_profiler import Profiler, profile

    with Profiler() as prof:
        session.execute(...)
    prof.print_stats()
"""

from sqlalchemy_profiler.profiler import Profiler, QueryRecord, profile
from sqlalchemy_profiler.renderers.console import ConsoleRenderer
from sqlalchemy_profiler.renderers.html import HtmlRenderer
from sqlalchemy_profiler.renderers.json import JsonRenderer

__all__ = [
    "Profiler",
    "QueryRecord",
    "profile",
    "ConsoleRenderer",
    "HtmlRenderer",
    "JsonRenderer",
]
