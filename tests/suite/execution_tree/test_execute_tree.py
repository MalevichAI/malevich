import numpy as np
from malevich._autoflow.tree import ExecutionTree

def test_execute_tree():
    for _ in range(50):
        tree = ExecutionTree()
        mat = np.zeros((11, 11))
        edges = [(1, 2)]
        tree.put_edge(1, 2)
        for i in range(2, 10):
            for n in range(np.random.randint(1, 10-i+1)):
                to = np.random.randint(i+1, 11)
                if not mat[i][to]:
                    tree.put_edge(i, to)
                    edges.append((i, to))
                    mat[i][to] = 1
        nodes = tree.topsort()

        for i in range(len(nodes)-1):
            for j in range(i+1, len(nodes)):
                if mat[i][j]:
                    assert (i, j) in edges and (j, i) not in edges, (
                        f"There's an edge from {j} to {i}, but {i} stands before {j}\n"
                        f"Edges: {edges}\n"
                        f"Nodes: {nodes}\n\n"
                    )