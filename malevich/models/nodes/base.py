from uuid import uuid4

from pydantic import BaseModel, Field


class BaseNode(BaseModel):
    uuid: str = Field(default_factory=lambda: uuid4().hex, repr=False)

    def __eq__(self, other: "BaseNode") -> bool:
        if not isinstance(other, BaseNode):
            return False
        return self.uuid == other.uuid
