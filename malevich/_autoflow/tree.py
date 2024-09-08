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

        self._node_map  = {}
        self._edge_map  = {}

    def remove_node_mapper(self, key: str) -> None:
        self._node_map.pop(key)

    def remove_edge_mapper(self, key: str) -> None:
        self._edge_map.pop(key)

    def clone_node_mappers(self, other: 'ExecutionTree') -> None:
        self._node_map = {**other._node_map}

    def clone_edge_mappers(self, other: 'ExecutionTree') -> None:
        self._edge_map = {**other._edge_map}

    def register_node_mapper(
        self, mapper: 'BaseNodeMapper', key: str | None = None
    ) -> None:
        """Register a node mapper

        Args:
            mapper (BaseNodeMapper): The node mapper to register
        """
        if key is None:
            key = id(mapper)
        self._node_map[key] = mapper

    def register_edge_mapper(
        self, mapper: 'BaseEdgeMapper', key: str | None = None
    ) -> None:
        """Register an edge mapper

        Args:
            mapper (BaseEdgeMapper): The edge mapper to register
        """
        if key is None:
            key = id(mapper)
        self._edge_map[key] = mapper

    def add_node(self, node: T, from_node: bool | None = None) -> None:
        """Add a node to the execution tree

        Args:
            node (T): The node to add
        """
        for mapper in self._node_map.values():
            mapper(node, from_node=from_node)

        if node not in self.nodes_:
            self.nodes_.add(node)


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
        if any(x[0] == callee and x[1] == caller and x[2] == link for x in self.tree):
            raise BadEdgeError("Edge already exists", (callee, caller, link))
        if any(x[0] == caller and x[1] == callee and x[2] == link for x in self.tree):
            raise BadEdgeError("Edge already exists", (callee, caller, link))
        if caller == callee:
            raise BadEdgeError("Self-edge", (callee, caller, link))

        self.add_node(callee, from_node=True)
        self.add_node(caller, from_node=False)

        for mapper in self._edge_map.values():
            mapper(callee, caller, link)
        self.tree.append((callee, caller, link))

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

    def roots(self) -> Iterable[tuple[int, T]]:
        return [
            (i, x) for i, x in enumerate(self.tree)
            if not any(y[1] == x[0] for y in self.tree)
        ]

    def edges_from(self, node: T) -> None:
        """Returns all edges starting from the specified node"""
        return [n for n in self.tree if n[0] == node]

    def edges_to(self, node: T) -> None:
        return [n for n in self.tree if n[1] == node]

    def traverse(self) -> Iterator[tuple[T, T, LinkType]]:
        """Traverse the execution tree

        The traversal is performed in a multi-source
        breadth-first manner.

        Returns:
            Generator[T]: Generator of nodes
        """

        # Mark visited nodes
        visited = [False] * len(self.tree)

        # Find roots
        roots = self.roots()

        # Traverse
        q = deque(maxlen=len(self.tree))
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
                    enumerate(self.tree)
                )
            )

    def topsort(self) -> Iterator[tuple[T, T, LinkType]]:
        sort_ = []
        s = [r[0] for _, r in self.roots()]
        edges = list(enumerate(self.tree))
        mask = [False] * len(edges)
        while s:
            n = s.pop()
            if n not in sort_:
                sort_.append(n)
            for i, (_, to, _) in filter(
                lambda x: x[1][0] == n and not mask[x[0]], edges
            ):
                mask[i] = True
                in_ = list(filter(lambda x: x[1][1] == to and not mask[x[0]], edges))
                add = True
                for j, (m, _, _) in in_:
                    if m != n and not mask[j]:
                        add = False
                if add:
                    s = [to, *s]

        return sort_


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

    def cast_link_types(self, type) -> None:
        """Cast the link types in the tree"""
        for i, (u, v, link) in enumerate(self.tree):
            self.tree[i] = (u, v, type(link))