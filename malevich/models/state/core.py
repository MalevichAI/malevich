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


class CoreParams(BaseModel):
    operation_id: str | None = None
    task_id: str | None = None
    core_host: str | None = None
    core_auth: tuple[str, str] | None = None
    base_config: core.Cfg | None = None
    base_config_id: str | None = None

    def __getitem__(self, key: str) -> Any:  # noqa: ANN401
        return getattr(self, key)

    def __setitem__(self, key: str, value: Any) -> None:  # noqa: ANN401
        setattr(self, key, value)

    def __contains__(self, key: str) -> bool:
        return hasattr(self, str(key))


class CoreInterpreterState(BaseModel):
    """State of the CoreInterpreterV2"""
    model_config = ConfigDict(
        arbitrary_types_allowed=True
    )

    service: SkipValidation[CoreService] | None = Field(default=None, exclude=True)
    """CoreService instance"""

    processors: dict[str, Processor] = {}
    """Mapping of operation aliases to processors"""

    conditions: dict[str, Condition] = {}
    """Mapping of condition aliases to processors"""

    collections: dict[str, Collection] = {}
    """Mapping of collection aliases to collections"""

    params: CoreParams = CoreParams()
    """Interpreter parameters"""

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


    def __deepcopy__(self, memo=None) -> "CoreInterpreterState":
        # Deepcopy the state without the service
        return CoreInterpreterState(**deepcopy(self.model_dump()), service=self.service)
