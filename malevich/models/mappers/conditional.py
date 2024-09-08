from malevich._autoflow.tracer import traced
from malevich.models.nodes.operation import OperationNode


class AddPositiveCondition:
    def __init__(self, uid) -> None:
        self.uid = uid

    def __call__(self, x: traced[OperationNode]) -> None:
        if not isinstance(x.owner, OperationNode):
            return
        if self.uid not in x.owner.should_be_true:
            x.owner.should_be_true = [*x.owner.should_be_true, self.uid]


class AddNegativeCondition:
    def __init__(self, uid) -> None:
        self.uid = uid

    def __call__(self, x: traced[OperationNode]) -> None:
        if not isinstance(x.owner, OperationNode):
            return
        if self.uid not in x.owner.should_be_false:
            x.owner.should_be_false = [*x.owner.should_be_false, self.uid]

