from typing import Generic, TypeVar

from pydantic import BaseModel, ConfigDict

from .._autoflow.tracer import traced

ArgumentLinkNodeType = TypeVar("ArgumentLinkNodeType")

class ArgumentLink(BaseModel, Generic[ArgumentLinkNodeType]):
    model_config = ConfigDict(
        arbitrary_types_allowed=True
    )

    index: int
    name: str
    compressed_edges: list[tuple['ArgumentLink', traced[ArgumentLinkNodeType]]] = []
    shadow_collection: traced | None = None
    is_compressed_edge: bool = False



    @property
    def compressed_nodes(
        self
    ) -> list[tuple['ArgumentLink', traced[ArgumentLinkNodeType]]]:
        # Compatability
        return self.compressed_edges
