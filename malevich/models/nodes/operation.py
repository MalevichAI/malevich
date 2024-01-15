import json

from .base import BaseNode


class OperationNode(BaseNode):
    operation_id: str
    config: dict = {}

    def get_senstivite_fields(self) -> dict[str, str]:
        return {"config": json.dumps(self.config)}

    def set_sensitive_fields(self, values: dict[str, str]) -> None:
        config = values.get("config")
        if config is None:
            return
        self.config = json.loads(config)
<<<<<<< Updated upstream
=======

    def short_info(self) -> str:
        p_ = ""
        if self.package_id:
            p_ += f"{self.package_id}"
        if self.processor_id:
            p_ += f"::{self.processor_id}"
        return f"{self.__class__.__name__}({self.operation_id}, {p_})"

    def __hash__(self) -> int:
        return super().__hash__()
>>>>>>> Stashed changes
