from typing import Any, Generic, TypeVar

from pydantic import ConfigDict

from malevich._autoflow.link import AutoflowLink

ArgumentLinkNodeType = TypeVar("ArgumentLinkNodeType")

class ArgumentLink(AutoflowLink, Generic[ArgumentLinkNodeType]):
    model_config = ConfigDict(
        arbitrary_types_allowed=True
    )


    compressed_edges: list[tuple['ArgumentLink', ArgumentLinkNodeType]] = []
    shadow_collection: Any = None
    is_compressed_edge: bool = False

    @property
    def compressed_nodes(
        self
    ) -> list[tuple['ArgumentLink', ArgumentLinkNodeType]]:
        # Compatability
        return self.compressed_edges
