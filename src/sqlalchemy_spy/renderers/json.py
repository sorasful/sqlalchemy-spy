from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy_spy.profiler import Profiler


class JsonRenderer:
    """Renders profiling results as JSON."""

    def __init__(self, *, indent: int | None = 2):
        self.indent = indent

    def render(self, profiler: "Profiler") -> str:
        """Return a JSON string representation of the profiling results."""
        return json.dumps(self._to_dict(profiler), indent=self.indent)

    def print(self, profiler: "Profiler") -> None:
        """Print the JSON output to stdout."""
        print(self.render(profiler))

    def _to_dict(self, profiler: "Profiler") -> dict[str, Any]:
        queries = []
        for q in profiler.queries:
            call_site = None
            if q.stack:
                frame = q.stack[-1]
                call_site = {
                    "file": frame.filename,
                    "line": frame.lineno,
                    "function": frame.name,
                }
            queries.append(
                {
                    "statement": q.statement,
                    "params": str(q.params),
                    "operation": q.operation,
                    "duration_ms": round(q.duration_ms, 4),
                    "error": q.error,
                    "call_site": call_site,
                    "explain_plan": q.explain_plan,
                }
            )

        return {
            "query_count": profiler.query_count,
            "total_ms": round(profiler.total_time_ms, 4),
            "avg_ms": (
                round(profiler.total_time_ms / profiler.query_count, 4)
                if profiler.query_count
                else 0
            ),
            "queries": queries,
        }
