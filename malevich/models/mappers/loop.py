from ..._autoflow.link import AutoflowLink
from ..nodes.base import BaseNode
from .base import BaseEdgeMapper


class LoopEdgeMapper(BaseEdgeMapper):
    def __init__(self, loop_nodes: list[BaseNode]):
        self._ln = loop_nodes

    def __call__(self, source, target, link) -> None:
        if source in self._ln:
            if isinstance(link, AutoflowLink):
                link.loop_argument = True
            else:
                raise ValueError('Invalid link in mapper')
        return True
