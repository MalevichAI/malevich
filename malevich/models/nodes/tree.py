import json
from typing import Iterable, Optional

from pydantic import ConfigDict

from ..._autoflow.tracer import traced
from ..._autoflow.tree import ExecutionTree
from ..argument import ArgumentLink
from .base import BaseNode


class TreeNode(BaseNode):
    tree: ExecutionTree[traced[BaseNode], ArgumentLink]
    results: Iterable[traced[BaseNode]] | traced[BaseNode] | None = None
    reverse_id: str
    name: str
    description: str = "Wonderful Flow!"
    underlying_node: Optional['BaseNode'] = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __eq__(self, __value: object) -> bool:
        return self.uuid == __value.uuid

    def get_senstivite_fields(self) -> dict[str, str]:
        in_nodes_fields = {}
        for u, v, _ in self.tree.tree:
            if u.owner.uuid not in in_nodes_fields:
                in_nodes_fields[u.owner.uuid] = u.owner.get_senstivite_fields()
            if v.owner.uuid not in in_nodes_fields:
                in_nodes_fields[v.owner.uuid] = v.owner.get_senstivite_fields()
        results_fields = {}
        if self.results is not None:
            if isinstance(self.results, Iterable):
                for node in self.results:
                    results_fields[node.owner.uuid] = node.owner.get_senstivite_fields()
            else:
                results_fields[self.results.owner.uuid] = (
                    self.results.owner.get_senstivite_fields()
                )
        return {
            "in_nodes_fields": json.dumps(in_nodes_fields),
            "results_fields": json.dumps(results_fields),
        }

    def set_sensitive_fields(self, values: dict[str, str]) -> None:
        in_nodes_fields = values.get("in_nodes_fields")
        if in_nodes_fields is not None:
            in_nodes_fields = json.loads(in_nodes_fields)
            for u, v, _ in self.tree.tree:
                u.owner.set_sensitive_fields(in_nodes_fields[u.owner.uuid])
                v.owner.set_sensitive_fields(in_nodes_fields[v.owner.uuid])
        results_fields = values.get("results_fields")
        if results_fields is not None:
            results_fields = json.loads(results_fields)
            if isinstance(self.results, Iterable):
                for node in self.results:
                    node.owner.set_sensitive_fields(results_fields[node.owner.uuid])
            else:
                self.results.owner.set_sensitive_fields(results_fields[self.results.owner.uuid])

    def __hash__(self) -> int:
        return super().__hash__()
