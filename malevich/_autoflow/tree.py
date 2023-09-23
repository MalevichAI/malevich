class ExecutionTree:
    tree: list[tuple[str, str]] = []


    def put_edge(self, caller: str, callee: str) -> None:
        self.tree.append((caller, callee))
