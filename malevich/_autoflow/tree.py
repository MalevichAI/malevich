from collections import deque
from typing import Any, Generic, Iterable, Iterator, TypeVar

T = TypeVar("T")

class ExecutionTree(Generic[T]):
    tree: list[tuple[T, T]] = []


    def put_edge(self, caller: T, callee: T, link: Any = None) -> None:  # noqa: ANN401
        self.tree.append((caller, callee, link))

    def traverse(self) -> Iterator[tuple[T, T, Any]]:
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
            j, node = q.pop()
            yield node
            visited[j] = True

            q.extend(
                filter(
                    lambda x: x[1][0] == node[1] and not visited[x[0]],
                    enumerate(graph)
                )
            )


    def leaves(self) -> Iterable[T]:
        return (
            x[1] for x in self.traverse()
            if not any(y[0] == x[1] for y in self.tree)
        )
