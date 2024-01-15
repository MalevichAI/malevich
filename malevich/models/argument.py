from pydantic import BaseModel, ConfigDict

from .._autoflow.tracer import traced
from .nodes.base import BaseNode


class ArgumentLink(BaseModel):
    index: int
    name: str
    compressed_edges: list[tuple['ArgumentLink', traced[BaseNode]]] = []
    is_compressed_edge: bool = False
    model_config = ConfigDict(
        arbitrary_types_allowed=True
    )


    @property
    def compressed_nodes(
        self
    ) -> list[tuple['ArgumentLink', traced[BaseNode]]]:
        # Compatability
        return self.compressed_edges
