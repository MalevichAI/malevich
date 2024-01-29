from collections import deque
from typing import Any, Generic, Iterable, Iterator, Optional, TypeVar

T = TypeVar("T")
LinkType = TypeVar("LinkType", bound=Any)


class BadEdgeError(Exception):
    def __init__(self, message: str, edge: tuple[T, T, LinkType]) -> None:
        super().__init__(message)
        self.edge = edge

    def __str__(self) -> str:
        return f"{super().__str__()}: {self.edge}"


class ExecutionTree(Generic[T, LinkType]):
    """An utility class holding a low-level representation of the execution graph

    The class provides a set of basic operations on the graph, such as
    adding edges, pruning, and traversing.

    The tree is stored both as set of nodes and as a list of edges. The nodes
    can be any hashable and comparable class, while the edges are triples
    of the form `(callee, caller, link)` where `caller` and `callee` are
    nodes and `link` is an arbitrary object.

    The connection is directed from `callee` to `caller`. This is
    because :code:`caller(callee)` notation requires the callee to be
    evaluated first.

    .. mermaid::

        graph LR
            Callee --> Caller

    """

    def __init__(self, tree: Optional[list[tuple[T, T, LinkType]]] = None) -> None:
        if tree is not None:
            self.tree = tree
        else:
            self.tree = []
        self.nodes_ = set()
        for u, v, _ in self.tree:
            self.nodes_.add(u)
            self.nodes_.add(v)

    def put_edge(self, callee: T, caller: T, link: LinkType = None) -> None:
        """Add an edge to the execution tree

        Args:
            callee (T): The source node (a.k.a. Callee)
            caller (T): The target node (a.k.a. caller)
            link (LinkType, optional):
                An arbitrary object that represents the edge payload
                (e.g. argument name). Defaults to None.

        Raises:
            BadEdgeError: If the edge already exists, or if the edge is a self-edge
        """
        if any(x[0] == callee and x[1] == caller for x in self.tree):
            raise BadEdgeError("Edge already exists", (callee, caller, link))
        if any(x[0] == caller and x[1] == callee for x in self.tree):
            raise BadEdgeError("Edge already exists", (callee, caller, link))
        if caller == callee:
            raise BadEdgeError("Self-edge", (callee, caller, link))
        self.tree.append((callee, caller, link))
        self.nodes_.add(callee)
        self.nodes_.add(caller)

    def prune(self, outer_nodes: list[T]) -> None:
        """Removes specified nodes from the execution tree

        Args:
            outer_nodes (list[T]): List of nodes to remove
        """
        self.tree = [
            x for x in self.tree
            if x[0] not in outer_nodes
        ]

        self.nodes_ = set([
            x for x in self.nodes_
            if x not in outer_nodes
        ])

    def edges_from(self, node: T) -> None:
        """Returns all edges starting from the specified node"""
        return [n for n in self.tree if n[0] == node]

    def traverse(self) -> Iterator[tuple[T, T, LinkType]]:
        """Traverse the execution tree

        The traversal is performed in a multi-source
        breadth-first manner.

        Returns:
            Generator[T]: Generator of nodes
        """
        graph = self.tree

        # Mark visited nodes
        visited = [False] * len(graph)

        # Find roots
        roots = [
            (i, x) for i, x in enumerate(graph)
            if not any(y[1] == x[0] for y in graph)
        ]

        # Traverse
        q = deque(maxlen=len(graph))
        for i, r in roots:
            # BFS
            q.append((i, r,))

        while q:
            j, edge = q.popleft()

            if visited[j]:
                continue

            yield edge
            visited[j] = True

            q.extend(
                filter(
                    lambda x: x[1][0] == edge[1] and not visited[x[0]],
                    enumerate(graph)
                )
            )

    def leaves(self) -> Iterable[T]:
        """Returns all leaves of the execution tree"""
        return (
            x[1] for x in self.traverse()
            if not any(y[0] == x[1] for y in self.tree)
        )

    @staticmethod
    def connected(a: 'ExecutionTree', b: 'ExecutionTree') -> bool:
        """Checks if two execution trees are connected"""
        return any(
            x[0] == b and x[1] == a for x in a.traverse()
        ) or any(
            x[0] == a and x[1] == b for x in b.traverse()
        )

    def nodes(self) -> Iterable[T]:
        """Returns all nodes of the execution tree"""
        return list(self.nodes_)
