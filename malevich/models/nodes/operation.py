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
