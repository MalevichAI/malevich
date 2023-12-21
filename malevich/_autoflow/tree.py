from collections import deque
from typing import Any, Generic, Iterable, Iterator, Optional, TypeVar

T = TypeVar("T")
LinkType = TypeVar("LinkType", bound=Any)

class ExecutionTree(Generic[T, LinkType]):
    # tree: list[tuple[T, T, Any]] = []

    def __init__(self, tree: Optional[list[tuple[T, T, LinkType]]] = None) -> None:
        if tree is not None:
            self.tree = tree
        else:
            self.tree = []

    def put_edge(self, caller: T, callee: T, link: LinkType = None) -> None:
        self.tree.append((caller, callee, link))

    def prune(self, outer_nodes: Optional[list[T]] = None) -> None:
        self.tree = [
            x for x in self.tree
            if x[0] not in outer_nodes
        ]

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


