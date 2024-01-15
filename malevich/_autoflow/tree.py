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

    def __init__(self, tree: Optional[list[tuple[T, T, LinkType]]] = None) -> None:
        if tree is not None:
            self.tree = tree
        else:
            self.tree = []
        self.nodes_ = set()
        for u, v, _ in self.tree:
            self.nodes_.add(u)
            self.nodes_.add(v)

    def put_edge(self, caller: T, callee: T, link: LinkType = None) -> None:
        if any(x[0] == caller and x[1] == callee for x in self.tree):
            raise BadEdgeError("Edge already exists", (caller, callee, link))
        if any(x[0] == callee and x[1] == caller for x in self.tree):
            raise BadEdgeError("Edge already exists", (caller, callee, link))
        if callee == caller:
            raise BadEdgeError("Self-edge", (caller, callee, link))
        self.tree.append((caller, callee, link))
        self.nodes_.add(caller)
        self.nodes_.add(callee)

    def prune(self, outer_nodes: Optional[list[T]] = None) -> None:
        self.tree = [
            x for x in self.tree
            if x[0] not in outer_nodes
        ]

        self.nodes_ = set([
            x for x in self.nodes_
            if x not in outer_nodes
        ])


    def edges_from(self, node: T) -> None:
        return [n for n in self.tree if n[0] == node]

    def traverse(self) -> Iterator[tuple[T, T, LinkType]]:
        """Traverse the execution tree in a determenistic order

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
        return (
            x[1] for x in self.traverse()
            if not any(y[0] == x[1] for y in self.tree)
        )

    @staticmethod
    def connected(a: 'ExecutionTree', b: 'ExecutionTree') -> bool:
        return any(
            x[0] == b and x[1] == a for x in a.traverse()
        ) or any(
            x[0] == a and x[1] == b for x in b.traverse()
        )

    def nodes(self) -> Iterable[T]:
        return list(self.nodes_)