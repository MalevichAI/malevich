from pydantic import BaseModel, ConfigDict

from .types import TracedNode


class ArgumentLink(BaseModel):
    index: int
    name: str
    compressed_nodes: list[tuple['ArgumentLink', TracedNode]] = []
    is_compressed_edge: bool = False
    model_config = ConfigDict(
        arbitrary_types_allowed=True
    )
