from typing import Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy_spy import Profiler


class BaseRender(Protocol):
    def render(self, profiler: "Profiler") -> None: ...
