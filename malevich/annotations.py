from ._autoflow.tracer import traced
from .models.nodes.asset import AssetNode
from .models.nodes.collection import CollectionNode
from .models.nodes.operation import OperationNode


class OpResult(traced[OperationNode]):
    """A class to represent a result of operation

    This serves as an annotation for the return type of the function.
    The result might be a collection (table), a series of collection or
    an asset. All the cases represented by a single object.
    """

class Collection(traced[CollectionNode]):
    """A class to represent a collection (table)"""

class Asset(traced[AssetNode]):
    """A class to represent an asset"""
