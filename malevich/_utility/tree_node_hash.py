from hashlib import sha256

from ..models.nodes.asset import AssetNode
from ..models.nodes.collection import CollectionNode
from ..models.nodes.document import DocumentNode
from ..models.nodes.operation import OperationNode
from ..models.nodes.tree import TreeNode
from ..types import FlowTree
from .tree import unwrap_tree


def get_tree_node_hash(tree_node: TreeNode | FlowTree) -> str:
    all_edges = set()
    tree = tree_node.tree if isinstance(tree_node, TreeNode) else tree_node
    for from_, to_, link_ in unwrap_tree(tree).tree:
        from_ = from_.owner
        to_ = to_.owner
        if isinstance(from_, OperationNode):
            fk = from_.processor_id
        elif isinstance(from_, CollectionNode):
            fk = from_.collection.collection_id
        elif isinstance(from_, AssetNode):
            fk = from_.name
        elif isinstance(from_, DocumentNode):
            fk = from_.magic() if from_.document is not None else from_.reverse_id

        if isinstance(to_, OperationNode):
            tk = to_.processor_id
        elif isinstance(to_, CollectionNode):
            tk = to_.collection.collection_id
        elif isinstance(to_, AssetNode):
            tk = to_.name
        elif isinstance(to_, DocumentNode):
            tk = to_.magic() if to_.document is not None else to_.reverse_id

        all_edges.add((fk, tk, link_.index))

    all_edges = sorted(all_edges, key=lambda x: x[2])
    return sha256(str(all_edges).encode()).hexdigest()
