from collections import deque
from typing import Any, List, Tuple


def flatdict(d: dict) -> List[Tuple[Tuple[str, ...], Any]]:
    """
    Flattens a nested dictionary into a list of tuples,
    where each tuple contains the path to a leaf node and its value.

    Args:
        d (dict): The dictionary to flatten.

    Returns:
        list:
            A list of tuples, where each tuple contains
            the path to a leaf node and its value.
    """
    res = []
    q = deque()

    q.extend([((k,), v) for k, v in d.items()])
    while q:
        path, value = q.pop()
        if isinstance(value, dict):
            q.extend([((*path, k), v) for k, v in value.items()])
        elif isinstance(value, list):
            q.extend([((*path, k), v) for k, v in enumerate(value)])
        else:
            res.append(
                (
                    path,
                    value,
                )
            )
    return res
