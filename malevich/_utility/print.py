import rich

from .._autoflow.tree import ExecutionTree


def print_tree_traverse(tree: ExecutionTree) -> None:
    rich.print('[b yellow]Tree Traverse[/b yellow] (Multi-source BFS):')
    for u, v, (link, arg) in tree.traverse():
        rich.print(f'\t[blue]{u.__repr__()}[/blue] --[yellow]({link}, {arg})[/yellow]--> [blue]{v.__repr__()}[/blue]')  # noqa: E501


def print_tree_edges(tree: ExecutionTree) -> None:
    rich.print('[b yellow]Tree Edges:[/b yellow]')
    for u, v, (link, arg) in tree.tree:
        rich.print(f'\t[blue]{u.__repr__()}[/blue] --[yellow]({link}, {arg})[/yellow]--> [blue]{v.__repr__()}[/blue]')  # noqa: E501
