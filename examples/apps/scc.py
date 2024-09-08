from malevich.square import processor, scheme, Docs, Doc, Context
from pydantic import BaseModel


@scheme()
class Node(BaseModel):
    """Scheme of a node in a graph

    Fields:
        node_id (int): ID of a node
        edges_to (list[int]): directed edges to nodes this node is connected to
    """
    node_id: int
    edges_to: list[int]


def dfs(cur_node: int, adj_list: list[list[int]], visited: list[bool]) -> list[int]:
    """Auxiliary method for depth-first search

    Args:
        cur_node (int): ID of a node in which the search is located currently
        adj_list (list[list[int]]): full adjacent list of a graph. An entry at position N corresponds to a list of IDs of the nodes that have an edge from the node N.
        visited (list[bool]): contains a status of each node N. If the entry at position N is `True`, the node was visited already.

    Returns:
        list[int]: list of nodes that were visited during the search
    """
    visited[cur_node] = True
    result = []
    for node in adj_list[cur_node]:
        if not visited[node]:
            result.extend(dfs(node, adj_list, visited))
    result.append(cur_node)
    return result


@processor()
def create_condensation_graph(graph: Docs[Node], context: Context) -> Docs[Node]:
    """Processor for extracting a condensed graph from the given directed graph. Each node in a condensed graph represents a strongly connected component in the initial graph.
    Strongly connected component in a directed graph represents a set of nodes, where every node can be reached from any other node.

    Args:
        graph (Docs[Node]): collection of nodes in a JSON format
        context (Context): context. Not used in a processor

    Returns:
        Docs[Node]: extracted condensed graph from the given one. Should follow the same format as the input graph
    """
    nodes = graph.parse(recursive=True)
    ... # Some logic there
    condensed_graph = [{
        'node_id': i + 1,
        'edges_to': [i - 1, i + 2]
    } for i in range(0, len(nodes) // 2)] # Answer format
    return condensed_graph
    
    
        
    