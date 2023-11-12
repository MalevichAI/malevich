from malevich_space.schema.schema import SchemaMetadata

from ..collection import Collection
from .base import BaseNode


class CollectionNode(BaseNode):
    collection: Collection
    scheme: str | SchemaMetadata | None = None
