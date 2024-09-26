from inspect import isclass
from typing import Type, TypeVar

from pydantic import BaseModel

from malevich._autoflow import traced
from malevich.models import DocumentNode, DocumentOverride

DocumentArgType = TypeVar('DocumentArgType', bound=BaseModel)
_TypeOrValue = Type[DocumentArgType] | DocumentArgType | Type[dict] | dict
class document:  # noqa: N801
    @staticmethod
    def override(
        data: DocumentArgType,
    ) -> DocumentOverride:
        return DocumentOverride(data=data)

    def __new__(
        cls,
        reverse_id: str,
        type_or_value: _TypeOrValue,
        *,
        alias: str | None = None,
    ) -> traced[DocumentNode]:
        if type_or_value is None:
            type_or_value = dict

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
