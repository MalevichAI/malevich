from typing import Iterator, Optional
from uuid import uuid4

from pydantic import BaseModel, Field

from malevich.models.python_string import PythonString


class BaseNode(BaseModel):
    uuid: str = Field(default_factory=lambda: uuid4().hex)
    alias: Optional[PythonString] = Field(default_factory=lambda: None)

    def __eq__(self, other: "BaseNode") -> bool:
        if not isinstance(other, BaseNode):
            return False
        return self.uuid == other.uuid

    def get_senstivite_fields(self) -> dict[str, str]:
        return {}

    def set_sensitive_fields(self, values: dict[str, str]) -> None:
        pass

    def short_info(self) -> str:
        return f"{self.__class__.__name__}({self.uuid[:6]}, {self.alias})"

    def __hash__(self) -> int:
        return hash(self.uuid)

    def __iter__(self) -> Iterator[tuple[dict[str, bool] | None, 'BaseNode']]:
        return iter({None: self}.items())
