from typing import Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy_profiler import Profiler


class BaseRender(Protocol):
    def render(self, profiler: "Profiler") -> None: ...
