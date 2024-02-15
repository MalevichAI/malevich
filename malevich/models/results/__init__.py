import typing as _typing

from .base import BaseResult
from .core import *
from .space import *

# Possible result types
Result = _typing.Union[
    BaseResult,
    CoreLocalDFResult,
    CoreResult,
    SpaceCollectionResult
]


__all__ = [
    "BaseResult",
    "CoreLocalDFResult",
    "CoreResult",
    "CoreResultPayload",
    "SpaceCollectionResult",
    "Result",
]
