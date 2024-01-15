from .._autoflow.tracer import traced, tracedLike
from .._autoflow.tree import ExecutionTree
from ..models.argument import ArgumentLink
from ..models.nodes.base import BaseNode
from ..models.nodes.tree import TreeNode
from ..models.types import FlowTree

MAX_DEFLAT_DEPTH = 1024
def deflat_edges(
    link: ArgumentLink,
) -> list[tuple[ArgumentLink, traced[BaseNode]]]:
    edges_ = link.compressed_edges
    def _done(nodes_: tuple[ArgumentLink, traced[BaseNode]]) -> bool:
        return all([not isinstance(n.owner, TreeNode) for _, n in nodes_])

    i = 0
    while not _done(edges_) and i < MAX_DEFLAT_DEPTH:
        new_edges = []
        for link, node in edges_:
            if isinstance(node.owner, TreeNode) and link.compressed_edges is not None:
                new_edges.extend(link.compressed_edges)
            else:
                new_edges.append((link, node))
        edges_ = new_edges
        i += 1

    if i == MAX_DEFLAT_DEPTH:
        raise RecursionError(
            "Maximum recursion depth reached while deflating edges"
        )

    return edges_

def unwrap_tree(
    tree: FlowTree,
) -> FlowTree:
    """Merges all subtrees into one tree

    Args
        tree (FlowTree): Tree of any depth

    Returns:
        FlowTree: Unwrapped tree
    """
    edges = []
    unwraped = {}
    for edge in tree.traverse():
        u = edge[0].owner
        v = edge[1].owner
        if isinstance(v, TreeNode):
            if not unwraped.get(v.uuid, False):
                edges.extend(unwrap_tree(v.tree).tree)
                unwraped[v.uuid] = True
            for bridge in deflat_edges(edge[2]):
                f = tracedLike(u.underlying_node) \
                    if isinstance(u, TreeNode) else edge[0]
                edges.append(
                    (f, bridge[1], bridge[0])
                )
        elif isinstance(u, TreeNode):
            edges.append(
                (tracedLike(u.underlying_node), edge[1], edge[2]),)
        else:
            edges.append(edge)
    return ExecutionTree(edges)
