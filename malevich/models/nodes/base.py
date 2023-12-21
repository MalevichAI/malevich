from uuid import uuid4

import randomname as rn
from pydantic import BaseModel, Field


class BaseNode(BaseModel):
    uuid: str = Field(default_factory=lambda: uuid4().hex)
    alias: str = Field(default_factory=lambda: rn.get_name())

    def __eq__(self, other: "BaseNode") -> bool:
        if not isinstance(other, BaseNode):
            return False
        return self.uuid == other.uuid

    def get_senstivite_fields(self) -> dict[str, str]:
        return {}

    def set_sensitive_fields(self, values: dict[str, str]) -> None:
        pass
