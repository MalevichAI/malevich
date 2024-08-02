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
        type_or_value: Type[DocumentArgType] | DocumentArgType | dict,
        reverse_id: str,
        alias: str | None = None,
    ) -> traced[DocumentNode]:
        if isclass(type_or_value):
            if issubclass(type_or_value, BaseModel):
                return traced(
                    DocumentNode(
                        document_class=type_or_value,
                        reverse_id=reverse_id,
                        alias=alias
                    )
                )
            else:
                return traced(
                    DocumentNode(
                        document=type_or_value.__dict__,
                        document_class=dict,
                        reverse_id=reverse_id,
                        alias=alias
                    )
                )
        else:
            return traced(
                DocumentNode(
                    document=type_or_value,
                    document_class=type(type_or_value),
                    reverse_id=reverse_id,
                    alias=alias
                )
            )
