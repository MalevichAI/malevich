from typing import Callable, Generic, ParamSpec, TypeVar

Params = ParamSpec("Params")
R = TypeVar("R")

class FlowFunction(Generic[Params, R]):
    def __init__(
        self,
        f: Callable[Params, R],
        reverse_id: str,
        name: str,
        description: str,
    ) -> None:
        self._Captured = f
        self.reverse_id = reverse_id
        self.name = name
        self.description = description

    def __call__(self, *args: Params.args, **kwds: Params.kwargs) -> R:
        return self._Captured(*args, **kwds)

