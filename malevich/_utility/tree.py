from malevich._autoflow import ExecutionTree, traced, tracedLike
from malevich.models import ArgumentLink, BaseNode
from ..models.nodes.morph import MorphNode
from malevich.types import FlowTree

MAX_DEFLAT_DEPTH = 1024
def deflat_edges(
    link: ArgumentLink,
) -> list[tuple[ArgumentLink, traced[BaseNode]]]:
    from malevich.models import TreeNode
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
    from malevich.models import TreeNode

    if len(tree.nodes_) == 1:
        return tree

    edges = []
    unwraped = {}
    for edge in tree.traverse():
        u = edge[0].owner
        v = edge[1].owner
        link = edge[2]
        if isinstance(v, TreeNode):
            if not unwraped.get(v.uuid, False):
                for inner_edge in unwrap_tree(v.tree).traverse():
                    if inner_edge[0] == link.shadow_collection:
                        continue
                    edges.append(inner_edge)
                unwraped[v.uuid] = True

            for bridge in deflat_edges(edge[2]):
                f = (
                    tracedLike(u.underlying_node)
                    if isinstance(u, TreeNode)
                    else edge[0]
                )

                edges.append(
                    (f, bridge[1], bridge[0])
                )

        elif isinstance(u, TreeNode):
            edges.append(
                (tracedLike(u.underlying_node), edge[1], edge[2]),)
        else:
            edges.append(edge)

    w_out_mophed_trees = []
    for u, v, link in edges:
        if isinstance(u.owner, MorphNode):
            members = []
            for conds, node in u.owner:
                if isinstance(node, TreeNode):
                    node = node.underlying_node
                members.append((conds, node))
            u = tracedLike(MorphNode(members=members))
            u.owner.correct_self()
        if isinstance(v.owner, MorphNode):
            members = []
            for conds, node in v.owner:
                if isinstance(node, TreeNode):
                    node = node.underlying_node
                members.append((conds, node))
            v = tracedLike(MorphNode(members=members))
            v.owner.correct_self()
        w_out_mophed_trees.append((u, v, link))

    return ExecutionTree(w_out_mophed_trees)
