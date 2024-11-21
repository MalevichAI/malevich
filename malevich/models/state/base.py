from copy import deepcopy
from typing import Annotated, Any

import malevich_coretools as core
from malevich_coretools import Condition, Processor, Result
from pydantic import BaseModel, ConfigDict, Field, SkipValidation

from ..._core.service.service import CoreService
from ..collection import Collection
from ..nodes.asset import AssetNode
from ..nodes.collection import CollectionNode
from ..nodes.document import DocumentNode
from ..nodes.operation import OperationNode


class BaseCoreState(BaseModel):
    """State of the CoreInterpreterV2"""
    model_config = ConfigDict(
        arbitrary_types_allowed=True
    )

    processors: dict[str, Processor] = {}
    """Mapping of operation aliases to processors"""

    conditions: dict[str, Condition] = {}
    """Mapping of condition aliases to processors"""

    collections: dict[str, Collection] = {}
    """Mapping of collection aliases to collections"""

    operation_nodes: dict[str, OperationNode] = {}
    """Mapping of operation aliases to operation nodes"""

    collection_nodes: dict[str, CollectionNode] = {}
    """Mapping of collection aliases to collection nodes"""

    asset_nodes: dict[str, AssetNode] = {}
    """Mapping of asset aliases to asset nodes"""

    document_nodes: dict[str, DocumentNode] = {}
    """Mapping of asset aliases to asset nodes"""

    results: dict[str, list[Result]] = {}
    """Mapping of processor aliases to results"""