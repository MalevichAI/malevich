from typing import Generic, TypeVar

from pydantic import BaseModel, ConfigDict

from .._autoflow.tracer import traced

ArgumentLinkNodeType = TypeVar("ArgumentLinkNodeType")

class ArgumentLink(BaseModel, Generic[ArgumentLinkNodeType]):
    index: int
    name: str
    compressed_edges: list[tuple['ArgumentLink', traced[ArgumentLinkNodeType]]] = []
    is_compressed_edge: bool = False
    model_config = ConfigDict(
        arbitrary_types_allowed=True
    )


    @property
    def compressed_nodes(
        self
    ) -> list[tuple['ArgumentLink', traced[ArgumentLinkNodeType]]]:
        # Compatability
        return self.compressed_edges
