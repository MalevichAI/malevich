from __future__ import annotations

from typing import Iterator

from pydantic import BaseModel, ConfigDict

from malevich._autoflow.tracer import traced

from .base import BaseNode
from .operation import OperationNode


class MorphNode(BaseNode):
    members: list[tuple[dict[OperationNode, bool] | None, BaseNode]] = []

    def __iter__(self) -> Iterator[tuple[dict[OperationNode, bool] | None, BaseNode]]:
        return iter(self.members)

    @staticmethod
    def transfigure(
        node: BaseNode,
        condition: traced[OperationNode],
        value: bool
    ) -> MorphNode:
        members = []
        for conds, node in node:
            assert not isinstance(node, MorphNode), (
                "violation: MorphNode cannot be nested"
            )
            if conds is None:
                conds = {condition.owner: value}
            else:
                conds[condition.owner] = value
            members.append((conds, node))
        return MorphNode(members=members)


    @staticmethod
    def combined(
        alpha: BaseNode,
        beta: BaseNode
    ) -> MorphNode:
        members = []
        for conds, node in alpha:
            members.append((conds, node))
        for conds, node in beta:
            members.append((conds, node))
        return MorphNode(members=members)


    def correct_self(self) -> None:
        members = []
        for conds, node in self.members:
            if isinstance(node, MorphNode):
                node.correct_self()
                for ncond, nnode in node:
                    if ncond is None:
                        ncond = conds
                    else:
                        ncond.update(conds or {})
                    members.append((ncond, nnode))
            else:
                members.append((conds, node))
        self.members = members
