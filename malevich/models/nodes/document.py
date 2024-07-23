import hashlib
import json
from typing import Generic, Type, TypeVar

from pydantic import BaseModel, Field

from .base import BaseNode

DocumentClass = TypeVar('DocumentClass', bound=BaseModel)

class DocumentNode(BaseNode, Generic[DocumentClass]):
    reverse_id: str
    core_id: str | None = None
    document: DocumentClass | None
    document_class: Type[DocumentClass] | None = Field(None, exclude=True)

    def magic(self) -> str:
        data = json.dumps(self.document.model_dump(), sort_keys=True)
        return hashlib.sha256(data.encode()).hexdigest()

    def get_core_path(self) -> str:
        if self.core_id is None:
            raise RuntimeError(
                "Attempt to get core path of a document node without core_id"
            )

        return f"#{self.core_id}"
