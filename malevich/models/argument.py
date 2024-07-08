from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict

ArgumentLinkNodeType = TypeVar("ArgumentLinkNodeType")

class ArgumentLink(BaseModel, Generic[ArgumentLinkNodeType]):
    model_config = ConfigDict(
        arbitrary_types_allowed=True
    )

    index: int
    name: str
    compressed_edges: list[tuple['ArgumentLink', ArgumentLinkNodeType]] = []
    shadow_collection: Any = None
    is_compressed_edge: bool = False

    @property
    def compressed_nodes(
        self
    ) -> list[tuple['ArgumentLink', ArgumentLinkNodeType]]:
        # Compatability
        return self.compressed_edges
