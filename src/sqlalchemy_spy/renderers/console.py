from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy_spy.profiler import Profiler, QueryRecord

RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
GREEN = "\033[92m"

_CWD = Path.cwd()


def _print(text: str = "", code: str = "") -> None:
    print(f"{code}{text}{RESET}" if code else text)


def _styled(text: str, code: str) -> str:
    return f"{code}{text}{RESET}"


def _fmt_duration(ms: float) -> str:
    color = RED if ms >= 100 else YELLOW if ms >= 20 else GREEN
    return _styled(f"{ms:>8.2f}ms", color)


def _truncate_sql(statement: str, max_len: int = 55) -> str:
    sql = statement.strip().replace("\n", " ")
    return sql[: max_len - 3] + "..." if len(sql) > max_len else sql


def _short_path(filename: str) -> str:
    """Return a path relative to CWD when possible, otherwise just the filename."""
    try:
        return str(Path(filename).relative_to(_CWD))
    except ValueError:
        return Path(filename).name


# (filename, lineno, function_name) → [QueryRecord, ...]
_CallSiteKey = tuple[str, int, str]


def _group_by_callsite(
    queries: list[QueryRecord],
) -> dict[_CallSiteKey, list[QueryRecord]]:
    groups: dict[_CallSiteKey, list[QueryRecord]] = defaultdict(list)
    for q in queries:
        if q.stack:
            frame = q.stack[-1]
            if frame.lineno is not None:
                groups[(frame.filename, frame.lineno, frame.name)].append(q)
    return dict(groups)


class ConsoleRenderer:
    """Renders profiling results as a colored table in the terminal."""

    def __init__(
        self,
        *,
        top_slow: int = 5,
        show_stack: bool = True,
        show_callsites: bool = True,
        top_callsites: int = 5,
    ):
        self.top_slow = top_slow
        self.show_stack = show_stack
        self.show_callsites = show_callsites
        self.top_callsites = top_callsites

    def render(self, profiler: "Profiler") -> None:
        queries = profiler.queries
        n = len(queries)
        total = profiler.total_time_ms

        self._print_header(n, total)

        if not queries:
            _print("  (no queries recorded)", DIM)
            print()
            return

        self._print_query_table(queries)

        if self.top_slow and n > 0:
            self._print_slowest(queries, n)

        if self.show_callsites:
            groups = _group_by_callsite(queries)
            if groups:
                self._print_hot_paths(groups)
                self._print_slow_callsites(groups)

        self._print_summary(total, n, queries)

    def _print_header(self, n: int, total: float) -> None:
        label = f"quer{'y' if n == 1 else 'ies'}"
        print()
        _print("─" * 72, BOLD)
        _print(f"  SQLAlchemy Profiler  -  {n} {label}  -  total {total:.2f}ms", BOLD)
        _print("─" * 72, BOLD)

    def _print_query_table(self, queries: list[QueryRecord]) -> None:
        print()
        _print("#    Duration   Op        SQL", DIM)
        for i, q in enumerate(queries, 1):
            err = f" {_styled('[ERROR]', RED)}" if q.error else ""
            print(
                f"  {i:<3} {_fmt_duration(q.duration_ms)}"
                f"  {_styled(f'{q.operation:<8}', CYAN)}"
                f"  {_truncate_sql(q.statement)}{err}"
            )
            if q.error:
                print(f"  {' ' * 22}{_styled(q.error, RED)}")

    def _print_slowest(self, queries: list[QueryRecord], n: int) -> None:
        print()
        _print(f"  Top {min(self.top_slow, n)} slowest:", BOLD)
        slowest = sorted(queries, key=lambda q: q.duration_ms, reverse=True)[
            : self.top_slow
        ]
        for rank, q in enumerate(slowest, 1):
            print(
                f"  {rank}. {_fmt_duration(q.duration_ms)}  {_truncate_sql(q.statement, max_len=65)}"
            )
            if self.show_stack and q.stack:
                frame = q.stack[-1]
                _print(
                    f"  {' ' * 14}↳ {frame.filename}:{frame.lineno} in {frame.name}()",
                    DIM,
                )

    def _print_hot_paths(self, groups: dict[_CallSiteKey, list[QueryRecord]]) -> None:
        print()
        _print("  Hot paths - most queries:", BOLD)
        ranked = sorted(groups.items(), key=lambda item: len(item[1]), reverse=True)[
            : self.top_callsites
        ]
        for rank, ((filename, lineno, fn), qs) in enumerate(ranked, 1):
            count = _styled(f"{len(qs)}×", CYAN)
            total = _fmt_duration(sum(q.duration_ms for q in qs))
            location = _styled(f"{_short_path(filename)}:{lineno} in {fn}()", DIM)
            print(f"  {rank}. {count}  {total}  {location}")

    def _print_slow_callsites(
        self, groups: dict[_CallSiteKey, list[QueryRecord]]
    ) -> None:
        print()
        _print("  Hot paths - most time:", BOLD)
        ranked = sorted(
            groups.items(),
            key=lambda item: sum(q.duration_ms for q in item[1]),
            reverse=True,
        )[: self.top_callsites]
        for rank, ((filename, lineno, fn), qs) in enumerate(ranked, 1):
            total = _fmt_duration(sum(q.duration_ms for q in qs))
            count = _styled(f"({len(qs)}×)", DIM)
            location = _styled(f"{_short_path(filename)}:{lineno} in {fn}()", DIM)
            print(f"  {rank}. {total}  {count}  {location}")

    def _print_summary(self, total: float, n: int, queries: list[QueryRecord]) -> None:
        slowest_ms = max(q.duration_ms for q in queries)
        print()
        _print("─" * 72, BOLD)
        print(
            f"  Total: {total:.2f}ms  |  Avg: {total / n:.2f}ms  |  Slowest: {slowest_ms:.2f}ms"
        )
        _print("─" * 72, BOLD)
        print()
