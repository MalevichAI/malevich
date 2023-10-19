from .base import BaseNode


class OperationNode(BaseNode):
    operation_id: str
    config: dict = {}
