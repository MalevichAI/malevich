from collections import defaultdict
from enum import Enum
from typing import Any
from uuid import uuid4

from malevich_space.ops.component_manager import ComponentManager
from malevich_space.ops.space import SpaceOps
from malevich_space.schema.component import ComponentSchema
from malevich_space.schema.flow import (
    FlowSchema,
    InFlowComponentSchema,
    InFlowDependency,
    OpSchema,
)
from malevich_space.schema.host import LoadedHostSchema

from malevich.models import TreeNode


class SpaceAuxParams:
    task_id: str | None
    run_id: str | None
    core_task_id: str | None
    flow_id: str | None
    tree: TreeNode | None

    def __init__(self, **kwargs) -> None:
        self.task_id = kwargs.get('task_id', None)
        self.run_id = kwargs.get('run_id', None)
        self.core_task_id = kwargs.get('core_task_id', None)
        self.flow_id = kwargs.get('flow_id', None)
        self.tree = kwargs.get('tree', None)

    def __getitem__(self, key: str) -> Any:  # noqa: ANN401
        return getattr(self, key)

    def __setitem__(self, key: str, value: Any) -> None:  # noqa: ANN401
        setattr(self, key, value)

    def __contains__(self, key: str) -> bool:
        return hasattr(self, str(key))

class SpaceInterpreterState:
    """State of the Space interpreter."""

    def __init__(self) -> None:

        # A manager to operate with components
        self.component_manager: ComponentManager = None
        # Flow to be interpreted
        self.flow: FlowSchema = FlowSchema()
        # Space Operations (just for convenience, same as in component_manager)
        self.space: SpaceOps = None
        # Host to run the task
        self.host: LoadedHostSchema = None
        # In flow components``
        self.components: dict[str, InFlowComponentSchema | ComponentSchema] = {}
        # In flow components aliases
        # NOTE: deprecated in favor of node.alias
        self.components_alias: dict[str, str] = {}
        # In flow components configs
        self.components_config: dict[str, dict[str, str]] = {}
        # A mappping from node uuid to node type (collection or operation)
        self.node_to_operation: dict[str, str] = {}
        # A mapping from node uuid to selected operation in the operation component
        self.selected_operation: dict[str, OpSchema] = {}
        # A mapping from collection uid to CA uid for override (if new data is provided)
        self.collection_overrides: dict[str, str] = {}
        # A mapping from node uuid to dependencies
        self.dependencies: dict[str, list[InFlowDependency]] = defaultdict(list)
        # Unique id for the interpretation
        self.interpretation_id: str = uuid4().hex  # as in API interpreter xD
        # A dictionary for storing auxiliary information
        # task_id, flow_id, etc.
        self.aux: SpaceAuxParams = SpaceAuxParams()
        self.children_states: dict[str, SpaceInterpreterState] = {}

    def copy(self) -> 'SpaceInterpreterState':
        state = SpaceInterpreterState()
        state.component_manager = self.component_manager
        state.flow = self.flow
        state.components = self.components
        state.components_alias = self.components_alias
        state.components_config = self.components_config
        state.dependencies = self.dependencies
        state.aux = self.aux
        state.space = self.space
        state.node_to_operation = self.node_to_operation
        state.selected_operation = self.selected_operation
        state.interpretation_id = self.interpretation_id
        state.collection_overrides = self.collection_overrides
        state.host = self.host
        state.children_states = self.children_states
        return state
