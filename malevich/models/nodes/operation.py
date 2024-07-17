import json
from typing import Iterable, Optional

from .base import BaseNode


class OperationNodeSpawner:
    RUN_AWAY_ITERATOR_LIMIT = 32

    def __init__(self, base_operation: 'OperationNode') -> None:
        self._index = -1
        self._base = base_operation

    def __next__(self) -> 'OperationNode':
        if self._index > self.RUN_AWAY_ITERATOR_LIMIT:
            raise RuntimeError(
                "Trying to unpack operation result with "
                "exhausitve iterator (like list or for-loop). "
                "Such syntax is not supported, use definite unpacking: "
                f"first, second = {self._base.processor_id}(...)"
            )
        self._index += 1
        return self._base.model_copy(update={'subindex': self._index}, deep=False)


class OperationNode(BaseNode):
    operation_id: str
    processor_id: Optional[str] = None
    package_id: Optional[str] = None
    config: dict = {}
    subindex: list[int] | None = None
    is_condition: bool = False


    def get_senstivite_fields(self) -> dict[str, str]:
        return {"config": json.dumps(self.config)}

    def set_sensitive_fields(self, values: dict[str, str]) -> None:
        config = values.get("config")
        if config is None:
            return
        self.config = json.loads(config)

    def short_info(self) -> str:
        p_ = ""
        if self.package_id:
            p_ += f"{self.package_id}"
        if self.processor_id:
            p_ += f"::{self.processor_id}"
        return f"{self.__class__.__name__}({self.operation_id}, {p_})"

    def __hash__(self) -> int:
        return super().__hash__()

    def __iter__(self) -> Iterable['OperationNode']:
        return OperationNodeSpawner(self)

    def __getitem__(self, index: int | slice) -> 'OperationNode':
        if isinstance(index, slice):
            assert index.step is None, "Step is not supported"
            start = index.start or 0
            if index.stop is None or index.stop < 0:
                raise ValueError(
                    "Only positive stop index is supported for operation result "
                    "slicing. But either stop index is negative or not provided."
                )
            return self.model_copy(
                update={'subindex': [i for i in range(start, index.stop)]},
                deep=False
            )
        return self.model_copy(update={'subindex': [index]}, deep=False)
