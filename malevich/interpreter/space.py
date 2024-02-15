import re
from collections import defaultdict
from typing import Optional
from uuid import uuid4

from malevich_space.ops.component_manager import ComponentManager
from malevich_space.ops.space import SpaceOps
from malevich_space.schema import SpaceSetup, VersionMode
from malevich_space.schema.cfg import CfgSchema
from malevich_space.schema.collection_alias import CollectionAliasSchema
from malevich_space.schema.component import ComponentSchema
from malevich_space.schema.flow import (
    FlowSchema,
    InFlowAppSchema,
    InFlowComponentSchema,
    InFlowDependency,
    OpSchema,
    Terminal,
)
from malevich_space.schema.schema import SchemaMetadata

from malevich.models.nodes.tree import TreeNode

from .._autoflow import tracer as gn
from .._autoflow.tracer import traced
from .._utility.cache.manager import CacheManager
from .._utility.logging import LogLevel, cout
from .._utility.registry import Registry
from .._utility.space.space import resolve_setup
from ..interpreter.abstract import Interpreter
from ..manifest import ManifestManager
from ..models.actions import Action
from ..models.argument import ArgumentLink
from ..models.exceptions import InterpretationError
from ..models.nodes.base import BaseNode
from ..models.nodes.collection import CollectionNode
from ..models.nodes.operation import OperationNode
from ..models.preferences import VerbosityLevel
from ..models.state.space import NodeType, SpaceInterpreterState
from ..models.task.base import BaseTask
from ..models.task.interpreted.space import SpaceTask
from ..models.types import TracedNode

manf = ManifestManager()
reg = Registry()
cache = CacheManager()

_levels = [LogLevel.Info, LogLevel.Warning, LogLevel.Error, LogLevel.Debug]
_actions = [Action.Interpretation, Action.Preparation, Action.Run, Action.Results]

def _log(
    message: str,
    level: int = 0,
    action: int = 0,
    step: bool = False,
    *args,
) -> None:
    cout(
        _actions[action],
        message,
        verbosity=VerbosityLevel.AllSteps if step else VerbosityLevel.OnlyStatus,
        level=_levels[level],
        *args,
    )

names_ = defaultdict(lambda: 0)
def _name(base: str) -> int:
    names_[base] += 1
    return names_[base]



class SpaceInterpreter(Interpreter[SpaceInterpreterState, FlowSchema]):
    """
    Interpret flows to be added and uploaded to your Malevich Space workspace.

    .. note::

        The interpreter can only operate with dependencies installed with
        both `space` installer.

    .. warning::

        Interpreting flows with this interpreter will immediately upload
        the flow to your Malevich Space workspace. Use :meth:`.interpret`
        wisely.

    Interpretation is equivalent to the Add
    action or :code:`malevich space component add` command.

    Prepare
    ---------

    Preparation creates a deployment of the flow in the Malevich Space.
    It is equialent to the Build action or :code:`malevich space component build` and
    :code:`malevich space component boot` commands.

    Run
    -----

    Run executes the flow in the Malevich Space. It is equivalent to the
    Run action or :code:`malevich space component run` command.

    Stop
    ------

    Deletes a deployment of the flow in the Malevich Space. It is equivalent
    to the Stop action or :code:`malevich space component stop` command.

    Results
    -------

    Results is represented as a list of
    :class:`malevich.results.space.SpaceCollectionResult`
    objects.
    """
    supports_subtrees = True

    def prettify_collection_id(
        self,
        collection_id: str
    ) -> str:
        # Replace spaces with dashes
        _s = re.sub(r'\s+', '-', collection_id)
        # Lowercase
        _s = _s.lower()
        # Replace underscores with dashes
        return _s

    def prettify_collection_name(
        self,
        collection_name: str
    ) -> str:
        # Replace multiple spaces with one
        _s = re.sub(r'\s+', ' ', collection_name)
        # Replace dashes and underscores with spaces
        _s = re.sub(r'-', ' ', _s)
        # Title case
        return _s.title()

    def prettify_schema_id(self, schema_name: str) -> str:
        _s = re.sub(r'[\s-]+', ' ', schema_name)
        return _s.replace(' ', '').lower()

    def prettify_component_name(self, component_name: str) -> str:
        _s = re.sub(r'[\s-]+', ' ', component_name)
        return _s.title()

    def prettify_config_name(self, component_name: str) -> str:
        _s = re.sub(r'[\s-]+', ' ', component_name)
        return 'Meta Config for ' + _s.title()

    def prettify_config_id(
        self,
        component_name: str,
        rand: Optional[str] = None  # Random string
    ) -> str:
        rand = rand or uuid4().hex[:8]
        __b = 'meta-config-for-' + re.sub(r'[\s|_|-]+', ' ', component_name)
        if rand:
            __b += '-' + rand
        return __b

    def update_state(self, state: SpaceInterpreterState = None) -> None:
        """
        The state contains pydantic models, which are not deepcopyable.
        So, this method is used to update the state using custom `copy` method.
        """
        if state:
            self._state = state.copy()
        return state

    @property
    def state(self) -> SpaceInterpreterState:
        return self._state.copy()

    def __init__(
        self,
        setup: SpaceSetup = None,
        ops: SpaceOps = None,
        name: Optional[str] = None,
        reverse_id: Optional[str] = None,
        description: Optional[str] = None,
    ) -> None:
        """Space Interpreter

        Args:
            name (str): Name of the flow.
            reverse_id (str): Reverse id of the flow.
        """
        super().__init__()
        self._state = SpaceInterpreterState()
        if not setup and not ops:
            try:
                setup = resolve_setup(manf.query("space", resolve_secrets=True))
            except Exception as e:
                raise InterpretationError(
                    "Failed to resolve space setup. "
                    "Please check your manifest file.",
                    self, self._state
                ) from e

        space = ops or SpaceOps(setup)

        try:  # Local patch for space
            host_ = space.get_my_hosts(url=setup.host.conn_url)[0]
        except Exception:
            host_ = None

        if not host_:
            try:
                host_ = space.create_host(
                    alias=setup.host.alias or '',
                    conn_url=setup.host.conn_url
                )
            except KeyError as ke:
                if 'details' in ke.args[0]:
                    # Local patch for space
                    pass

        self._state.component_manager = ComponentManager(
            host=host_,
            space=space,
            comp_dir='./'
        )

        self._state.host = host_
        self._state.space = space

        # self._state.aux = {
        #     'name': name,
        #     'reverse_id': reverse_id,
        #     'description': description,
        # }

        self.update_state()

    def upload_collection(
        self,
        state: SpaceInterpreterState,
        node: CollectionNode
    ) -> str:
        """Uploads collection (and underlying scheme) to the space."""
        if not node.scheme:
            # Actually, I infer scheme from the collection
            # RFC: Should I allow to upload collection without scheme?
            raise InterpretationError(
                "To be able to use collection, it should have an attached schema",
                self, state
            )

        # Understanding scheme
        if isinstance(node.scheme, SchemaMetadata):
            # If scheme is already parsed
            core_id = node.scheme.core_id

            if not state.space.get_schema(core_id=core_id):
                # HACK: This will fall into another
                # if branch where the scheme will be
                # uploaded
                node.scheme = node.scheme.schema_data

        if isinstance(node.scheme, str):
            # If scheme is a string, then it should firstly
            # be uploaded
            core_id = self.prettify_schema_id(
                node.collection.collection_id
            ),

            schema_uid = state.space.create_scheme(
                # $core_id: String!, $raw: String!, $name: String
                core_id=core_id,
                raw=node.scheme,
                name=self.prettify_schema_id(
                    node.collection.collection_id
                )
            )
            # RFC: Should I allow to upload collection without scheme?
            assert schema_uid, "Failed to upload the schema"

        # Uploading collection
        alias = self.prettify_collection_name(
            node.collection.collection_id
        )

        # RFC!: Is it correct way to upload collection?
        uid = state.space.create_collection(
            host_id=state.host.uid,
            core_id=node.collection.collection_id,
            core_alias=alias,
            schema_core_id=core_id,
            docs=[
                row.to_json()
                for _, row in node.collection.collection_data.iterrows()
            ]
        )

        return uid

    def interpret(self, node: TreeNode, component: ComponentSchema) -> BaseTask:
        self._state.aux.tree = node
        return super().interpret(node, component)

    def before_interpret(self, state) -> SpaceInterpreterState:
        state.flow = FlowSchema()
        return state

    def create_node(  # Let's go!
        self, state: SpaceInterpreterState, node: gn.traced[BaseNode]
    ) -> SpaceInterpreterState:
        """Wraps node in the execution graph with a Malevich Space component schema.

        Args:
            state (SpaceInterpreterState): Interpreter state.
            node (gn.traced[BaseNode]): Node to wrap.

        Returns:
            SpaceInterpreterState: Interpreter state.
        """
        # TODO: The branch is too long, should refactor it

        if isinstance(node.owner, CollectionNode):  # If the node is a collection
            state.node_type[node.owner.uuid] = NodeType.COLLECTION
            core_schema_id = self.prettify_schema_id(
                node.owner.collection.collection_id
            )
            # Create a schema for the collection (only name and reverse id)
            # Maybe schema is already created
            if (schema := state.component_manager.space.get_schema(core_id=core_schema_id)) is None:  # noqa: E501
                # Create the schema if not created
                schema_id = state.component_manager.space.create_scheme(
                    # core.create_scheme(...)
                    core_id=core_schema_id,
                    # This is generated with wrapped
                    # class
                    # See malevich._meta.collection.collection()
                    raw=node.owner.scheme
                )

                # And another one!
                schema = state.component_manager.space.get_schema(
                    uid=schema_id, core_id=core_schema_id
                )

                _log(f"Schema {schema} is created", level=-1, action=0, step=True)

            # To upload the collection, it is required to
            # save the collection data in a csv file

            _, path = cache.space.probe_new_entry(
                node.owner.collection.collection_id,
                entry_group='collections/temp'
            )
            # Save the collection data in the csv file
            if not node.owner.collection.collection_data.empty:
                node.owner.collection.collection_data.to_csv(
                    path,
                    index=False
                )
            else:
                path = None

            # Try to get the collection component from the space
            component = state.space.get_parsed_component_by_reverse_id(
                # Using the collection id as the reverse id
                reverse_id=node.owner.collection.collection_id
            )
            alias_base = node.owner.collection.collection_id

            # If the component is not found (not uploaded yet)
            if component is None:
                # Wrap the collection in a component schema
                name = self.prettify_collection_name(
                    node.owner.collection.collection_id
                )
                comp = ComponentSchema(
                    name=name,
                    description=f"Meta collection {name}",
                    reverse_id=node.owner.collection.collection_id,
                    collection=CollectionAliasSchema(
                        core_alias=node.owner.collection.collection_id,
                        schema_core_id=schema.core_id,
                        path=path
                    )
                )
            else:
                # Collection already exists, so two
                # cases:
                # 1. Overriding with new data
                # 2. Using as it is
                coll_id = self.prettify_collection_id(
                    node.owner.collection.collection_id
                )
                if not node.owner.collection.collection_data.empty:
                    # Has some data, so override
                    uid = state.space.create_collection(
                        host_id=state.host.uid,
                        # core_id=f'override-{coll_id}-{state.interpretation_id}',
                        core_alias=f'override-{coll_id}-{state.interpretation_id}',
                        schema_core_id=schema.core_id,
                        docs=[
                            row.to_json()
                            for _, row in node.owner.collection.collection_data.iterrows()  # noqa: E501
                        ]
                    )
                    state.collection_overrides[component.collection.uid] = uid

                    comp = ComponentSchema(
                        reverse_id=component.reverse_id,
                        name=component.name,
                    )

        elif isinstance(node.owner, OperationNode):  # If the node is an operation
            state.node_type[node.owner.uuid] = NodeType.OPERATION

            # All the required information to interpret the node
            # should be in the registry
            extra = reg.get(node.owner.operation_id)

            # If no extra information is found, it means
            # local information is too old
            # (but it is user fault, not mine)
            if 'reverse_id' not in extra:
                raise InterpretationError(
                    "Trying to interpret an operation that is not installed "
                    "properly or extremely outdated. Try to reinstall "
                    "the operation. ",
                    self, state
                )

            alias_base = extra['reverse_id']

            # Get the component from the space (it should be there)
            component = state.component_manager.space.get_parsed_component_by_reverse_id(  # noqa: E501
                reverse_id=extra['reverse_id']
            )

            if not component:
                # How then you install it? Was it wiped? :(
                raise InterpretationError(
                    "Trying to interpret an operation that is not installed "
                    "properly or extremely outdated. Try to reinstall "
                    "the operation with\n\t"
                    f"`malevich install {extra['reverse_id']} --using space`",
                    self, state
                )

            state.components_config[node.owner.uuid] = node.owner.config
            comp = ComponentSchema(
                name=component.name,
                reverse_id=component.reverse_id,
                app=component.app
            )
            state.node_to_operation[node.owner.uuid] = node.owner.operation_id

        elif isinstance(node.owner, TreeNode):
            alias_base= node.owner.reverse_id
            if not state.children_states.get(node.owner.uuid, None):
                child_interpreter = SpaceInterpreter(
                    name=node.owner.name,
                    reverse_id=node.owner.reverse_id
                )

                child_interpreter.interpret(node.owner)
                child_state: SpaceInterpreterState = child_interpreter.state

                comp = ComponentSchema(
                    name=node.owner.name,
                    reverse_id=node.owner.reverse_id,
                    flow=child_state.flow,
                )

                state.children_states[node.owner.uuid] = child_state
            else:
                child_state = state.children_states[node.owner.uuid]
                comp = ComponentSchema(
                    name=node.owner.name,
                    reverse_id=node.owner.reverse_id,
                    flow=child_state.flow,
                )

        if not comp:
            raise InterpretationError(
                "Failed to interpret the node. This is a bug, please report it.",
            )

        state.components[node.owner.uuid] = comp
        state.components_alias[node.owner.uuid] = node.owner.alias or f'{alias_base} #{_name(alias_base)}'  # noqa: E501
        node.owner.alias = state.components_alias[node.owner.uuid]
        return state

    def create_dependency(
        self,
        state: SpaceInterpreterState,
        caller: TracedNode,
        callee: traced[OperationNode],
        link: ArgumentLink
    ) -> SpaceInterpreterState:
        """Creates a dependency between two nodes."""

        # Case Collection / App -> Flow

        if isinstance(callee.owner, TreeNode) and not isinstance(caller.owner, TreeNode):  # noqa: E501
            child = state.children_states[callee.owner.uuid]
            caller_alias = state.components_alias[caller.owner.uuid]
            inter_flow_map = {}

            bridges = link.compressed_nodes

            for _, to in bridges:
                inter_flow_map[caller_alias] = child.components_alias[to.owner.uuid]

            dependency = InFlowDependency(
                from_op_id=(
                    # provided by space installer
                    caller.owner.operation_id
                    if isinstance(caller.owner, OperationNode)
                    else None
                ),
                to_op_id=callee.owner.underlying_node.operation_id if isinstance(
                    callee.owner.underlying_node, OperationNode) else None,
                alias=state.components_alias[caller.owner.uuid],
                order=link.index,
                terminals=[
                    Terminal(
                        src=x,
                        target=y
                    ) for x, y in inter_flow_map.items()
                ]
            )
        elif isinstance(caller.owner, TreeNode) and not isinstance(callee.owner, TreeNode):  # noqa: E501
            child = state.children_states[caller.owner.uuid]
            callee_alias = state.components_alias[callee.owner.uuid]
            op: OperationNode = caller.owner.underlying_node
            inter_flow_map = {
                child.components_alias[op.uuid]: callee_alias
            }

            dependency = InFlowDependency(
                from_op_id=op.operation_id,
                to_op_id=(
                    # provided by space installer
                    callee.owner.operation_id
                    if isinstance(callee.owner, OperationNode)
                    else None
                ),
                alias=state.components_alias[caller.owner.uuid],
                order=link.index,
                terminals=[
                    Terminal(
                        src=x,
                        target=y
                    ) for x, y in inter_flow_map.items()
                ]
            )
        elif isinstance(caller.owner, TreeNode) and isinstance(callee.owner, TreeNode):
            left_op = caller.owner.underlying_node
            right_edges = link.compressed_nodes
            for rel, right_node in right_edges:
                state.dependencies[callee.owner.uuid].append(
                    InFlowDependency(
                        from_op_id=(
                            left_op.operation_id
                            if isinstance(left_op, OperationNode)
                            else None
                        ),
                        to_op_id=(
                            right_node.owner.operation_id
                            if isinstance(right_node.owner, OperationNode)
                            else None
                        ),
                        alias=state.components_alias[caller.owner.uuid],
                        order=rel.index,
                        terminals=[Terminal(
                            src=state.children_states[caller.owner.uuid].components_alias[left_op.uuid],
                            target=state.children_states[callee.owner.uuid].components_alias[right_node.owner.uuid]
                        )]
                    )
                )
            return state
        else:
            dependency = InFlowDependency(
                from_op_id=(
                    # provided by space installer
                    caller.owner.operation_id
                    if isinstance(caller.owner, OperationNode)
                    else None
                ),
                to_op_id=callee.owner.operation_id,
                alias=state.components_alias[caller.owner.uuid],
                order=link.index,
            )

        state.dependencies[callee.owner.uuid].append(dependency)

        return state

    def after_interpret(self, state: SpaceInterpreterState) -> SpaceInterpreterState:
        """Finishes the interpretation by adding components to the flow."""
        for uid, component in state.components.items():
            try:
                # I don't know why, but sometimes
                # it fails, so try/exc here
                loaded_component = state.component_manager.component(
                    component,
                    VersionMode.DEFAULT
                )
            except Exception as e:
                # If it fails, try to get the component
                # by reverse id
                loaded_component = state.component_manager.space.get_component_by_reverse_id(  # noqa: E501
                    component.reverse_id
                )

                # No components?(
                # Nothing to do here
                if loaded_component is None:
                    raise InterpretationError(
                        f"Failed to interpret the flow: component {component.name} "
                        "failed to load. ",
                        self, state
                    ) from e

            # If the component is an operation
            # and has config
            if uid in state.components_config:
                # RFC: Is it correct way to update config?
                space_config = CfgSchema(
                    readable_name=self.prettify_config_name(
                        component.reverse_id),
                    cfg_json=state.components_config.get(uid),
                    core_name=self.prettify_config_id(component.reverse_id),
                )
            else:
                space_config = None

            if uid in state.node_to_operation:
                extra = reg.get(state.node_to_operation[uid], {})
            else:
                extra = {}

            # Add the component to the flow
            state.flow.components = [
                *state.flow.components,
                InFlowComponentSchema(
                    reverse_id=component.reverse_id,
                    alias=state.components_alias[uid],
                    depends={
                        dep.alias: dep
                        for dep in state.dependencies[uid]
                    },
                    app=InFlowAppSchema(
                        active_op=[
                            OpSchema(
                                core_id=extra['processor_name'],
                                type='processor'
                            )
                        ]
                    ) if component.app is not None else None,
                    active_cfg=space_config,
                ),
            ]

        return state


    def get_task(
        self,
        state: SpaceInterpreterState
    ) -> BaseTask[SpaceInterpreterState]:
        if self._component is None:
            raise Exception("Expected _component to be not None")

        self._component.flow = state.flow

        component = state.component_manager.component(
            self._component,
            VersionMode.PATCH
        )

        return SpaceTask(
            state=self.state,
            component=component
        )
