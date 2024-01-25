import typing as _typing

from .base import BaseResult
from .core import *
from .space import *

# Possible result types
Result = list[
    _typing.Union[
        BaseResult,
        CoreLocalDFResult,
        CoreResult,
        CoreResultPayload,
        SpaceCollectionResult
    ]
]


__all__ = [
    "BaseResult",
    "CoreLocalDFResult",
    "CoreResult",
    "CoreResultPayload",
    "SpaceCollectionResult",
    "Result",
]
