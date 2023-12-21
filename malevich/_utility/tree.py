from .._autoflow.tracer import tracedLike
from .._autoflow.tree import ExecutionTree
from ..models.nodes.tree import TreeNode
from ..models.types import FlowTree


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
            for bridge in edge[2].compressed_nodes:
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
