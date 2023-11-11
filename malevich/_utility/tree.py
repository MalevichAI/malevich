from .._autoflow.tracer import traced, tracedLike
from .._autoflow.tree import ExecutionTree
from ..models.nodes.base import BaseNode
from ..models.nodes.tree import TreeNode


def unwrap_tree(
    tree: ExecutionTree[traced[BaseNode]]
) -> ExecutionTree[traced[BaseNode]]:
    """Merges all subtrees into one tree

    Args
        tree (ExecutionTree): Tree of any depth

    Returns:
        ExecutionTree: Unwrapped tree
    """
    edges = []
    unwraped = {}
    for edge in tree.traverse():
        if isinstance(edge[1].owner, TreeNode):
            if not unwraped.get(edge[1].owner.uuid, False):
                edges.extend(unwrap_tree(edge[1].owner.tree).tree)
                unwraped[edge[1].owner.uuid] = True
            for bridge in edge[2][1]:
                edges.append(
                    (edge[0], bridge[0], bridge[1])
                )
        elif isinstance(edge[0].owner, TreeNode):
            edges.append(
                (tracedLike(edge[0].owner.underlying_node), edge[1], edge[2]),)
        else:
            edges.append(edge)
    return ExecutionTree(edges)
