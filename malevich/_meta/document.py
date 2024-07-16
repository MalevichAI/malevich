from inspect import isclass
from typing import Type, TypeVar

from pydantic import BaseModel

from malevich._autoflow import traced
from malevich.models import DocumentNode, DocumentOverride

DocumentArgType = TypeVar('DocumentArgType', bound=BaseModel)

class document:  # noqa: N801
    @staticmethod
    def override(
        data: DocumentArgType,
    ) -> DocumentOverride:
        return DocumentOverride(data=data)

    def __new__(
        cls,
        type_or_model: Type[DocumentArgType] | DocumentArgType,
        reverse_id: str,
        alias: str | None = None,
    ) -> traced[DocumentNode[DocumentArgType]]:
        if isclass(type_or_model):
            if issubclass(type_or_model, BaseModel):
                return traced(
                    DocumentNode(
                        document_class=type_or_model,
                        reverse_id=reverse_id,
                        alias=alias
                    )
                )
            else:
                raise TypeError(
                    "Expected first argument of document(...) to be either "
                    "instance of pydantic BaseModel or a pydantic model class "
                    f"but got {type_or_model}"
                )
        else:
            return traced(
                DocumentNode(
                    document=type_or_model,
                    document_class=type(type_or_model),
                    reverse_id=reverse_id,
                    alias=alias
                )
            )
