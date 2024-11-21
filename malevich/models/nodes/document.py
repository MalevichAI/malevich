import hashlib
import json
from typing import Any, Generic, Type, TypeVar

from pydantic import BaseModel, Field

from .base import BaseNode

DocumentClass = TypeVar('DocumentClass', bound=BaseModel)

class DocumentNode(BaseNode):
    reverse_id: str
    core_id: str | None = None
    document: Any = None
    document_class: Type | Type[dict] | None = Field(None, exclude=True)

    def magic(self) -> str:
        return hashlib.sha256(self.dump_document_json().encode()).hexdigest()

    def dump_document_json(self) -> str:
        if isinstance(self.document, BaseModel):
            data = json.dumps(self.document.model_dump(), sort_keys=True)
        else:
            data = json.dumps(self.document, sort_keys=True)
        return data

    def get_core_path(self) -> str:
        if self.core_id is None:
            raise RuntimeError(
                "Attempt to get core path of a document node without core_id"
            )

        return f"#{self.core_id}"
