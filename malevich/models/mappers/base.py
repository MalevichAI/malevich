from typing import Any


class BaseNodeMapper:
    def __call__(self, node) -> None:
        pass


class BaseEdgeMapper:
    def __call__(self, source, target, link) -> None:
        pass
