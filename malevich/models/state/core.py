import uuid
from collections import defaultdict
from typing import Any

import malevich_coretools as core
from malevich_space.schema import ComponentSchema

from ..._utility.registry import Registry
from ...manifest import ManifestManager
from ..argument import ArgumentLink
from ..nodes.base import BaseNode


class CoreParams:
    operation_id: str
    task_id: str
    core_host: str
    core_auth: tuple[str, str]
    base_config: core.Cfg
    base_config_id: str

    def __init__(self, **kwargs) -> None:
        self.operation_id = kwargs.get('operation_id', None)
        self.task_id = kwargs.get('task_id', None)
        self.core_host = kwargs.get('core_host', None)
        self.core_auth = kwargs.get('core_auth', None)
        self.base_config = kwargs.get('base_config', None)
        self.base_config_id = kwargs.get('base_config_id', None)

    def __getitem__(self, key: str) -> Any:  # noqa: ANN401
        return getattr(self, key)

    def __setitem__(self, key: str, value: Any) -> None:  # noqa: ANN401
        setattr(self, key, value)

    def __contains__(self, key: str) -> bool:
        return hasattr(self, str(key))


class CoreInterpreterState:
    """State of the CoreInterpreter"""

    def __init__(self) -> None:
        # Involved operations
        self.ops: dict[str, BaseNode] = {}
        # Dependencies (same keys as in self.ops)
        self.depends: dict[str, list[tuple[BaseNode, ArgumentLink]]] = defaultdict(list)
        # Registry reference (just a shortcut)
        self.reg = Registry()
        # Manifest manager reference (just a shortcut)
        self.manf = ManifestManager()
        # Task configuration
        self.cfg = core.Cfg()
        # Collections (and assets) (key: operation_id, value: (local_id, core_id,))
        self.collections: dict[str, tuple[str, str]] = {}
        # Uploaded operations (key: operation_id, value: core_op_id)
        self.core_ops: dict[str, BaseNode] = {}
        # Interpreter parameters
        self.params: CoreParams = CoreParams()
        # Results
        self.results: dict[str, str] = {}
        # Interpretation ID
        self.interpretation_id: str = uuid.uuid4().hex
        # App args
        self.app_args: dict[str, Any] = {}
        # Collections
        self.extra_colls: dict[str, dict[str, str]] = defaultdict(dict)
        # Alias to task id
        self.task_aliases: dict[str, str] = {}
        # Component
        self.component: ComponentSchema | None = None
