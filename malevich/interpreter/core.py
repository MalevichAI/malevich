import json
from collections import defaultdict
from typing import Optional

import malevich_coretools as core
from malevich_coretools import (
    AlternativeArgument,
    Argument,
    Condition,
    JsonImage,
    Processor,
    Result,
)
from malevich_space.schema import ComponentSchema

from malevich._autoflow import traced
from malevich._core import (
    result_collection_name,
)
from malevich._core.service.service import CoreService
from malevich._utility import CacheManager, LogLevel, Registry, cout, unique
from malevich.constants import CORE_INTERPRETER_IN_APP_INFO_KEY, DEFAULT_CORE_HOST
from malevich.interpreter import Interpreter
from malevich.models import (
    Action,
    ArgumentLink,
    AssetNode,
    BaseNode,
    CollectionNode,
    CoreInterpreterState,
    CoreRegistryEntry,
    CoreTask,
    DocumentNode,
    InjectedAppInfo,
    InterpretationError,
    OperationNode,
    TreeNode,
    VerbosityLevel,
)

cache = CacheManager()
registry = Registry()

_levels = [
    LogLevel.Info,
    LogLevel.Warning,
    LogLevel.Error,
    LogLevel.Debug
]

_actions = [
    Action.Interpretation,
    Action.Preparation,
    Action.Run,
    Action.Results
]


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



class CoreInterpreter(Interpreter[CoreInterpreterState, CoreTask]):
    """Interpret flows to be run on Malevich Core

    Malevich Core is a computational cloud of Malevich. It provides
    low-level access to the computational resources of Malevich. This
    interpreter utilizes its API to run your flows.

    .. note::

        The interpreter can operate with dependencies installed with
        both `image` and `space` installer.

    Prepare
    ---------

    The prepare hook is a simple proxy to the `task_prepare` function
    of Malevich Core API. All the parameters are optional.

    Options:

    *   :code:`with_logs (bool)`:
            If set, return prepare logs if True after end
    *   :code:`debug_mode (bool)`:
            If set, displays additional information about errors
    *   :code:`info_url (str)`:
            URL to which the request is sent. If not specified, the default url is used.
            Rewrite msg_url from configuration if exist
    *   :code:`core_manage (bool)`:
            If set, the requests will be managed by Core, otherwise by the DAG Manager.
    *   :code:`with_show (bool)`:
            Show results (like for each operation, default equals with_logs arg)
    *   :code:`profile_mode (str)`:
            If set, provides more information in logs.
            Possible modes: :code:`no`, :code:`all`, :code:`time`,
            :code:`df_info`, :code:`df_show`

    Run
    -----

    The run hook is a simple proxy to the `task_run` function
    of Malevich Core API. All the parameters are optional.

    Options:

        **Interpreter-specific parameters**

        * :code:`run_id (str)`:
            A custom identifier for the run. If not specified, a random
            identifier is generated.
        * :code:`overrides (dict[str, str])`:
            A dictionary of overrides for the collections. The keys are
            collection names, the values are collection IDs. The option
            is managed by Runners, but can be provided manually.

        **Core-API parameters**

        * :code:`with_logs (bool)`:
            If set return prepare logs if True after end
        * :code:`debug_mode (bool)`:
            If set, displays additional information about errors
        * :code:`with_show (bool)`:
            Show results in logs
        * :code:`profile_mode (str)`:
            If set, provides more information in logs.
            Possible modes: :code:`no`, :code:`all`, :code:`time`,
            :code:`df_info`, :code:`df_show`


    Stop
    ------

    The stop hook is a simple proxy to the `task_stop` function.
    It stops the task and does not have any options.

    Results
    -------

    Results is represented as a list of :class:`malevich.results.core.CoreResult`
    objects. Each object contains a payload which can be an asset, a dataframe or
    a list of such. Use :code:`.get_df()`, :code:`.get_binary()`, :code:`.get_dfs()`
    and :code:`.get_binary_dir()` to extract the results.

    Configure
    ---------

    You can mark an operation to be run on a specific platform. To do so,
    set :code:`platform` and :code:`platform_settings` parameters in the
    :code:`.configure()` method. The :code:`platform` parameter can be
    :code:`base` or :code:`vast`. The :code:`platform_settings` parameter
    is a dictionary of settings for the platform. The settings are
    platform-specific.

    """

    supports_subtrees = False

    def __init__(
        self,
        core_auth: tuple[str, str],
        core_host: str = DEFAULT_CORE_HOST,
    ) -> None:
        super().__init__(CoreInterpreterState())
        # TODO: Remove this (deprecated in favor of service)
        self.__core_host = core_host
        self.__core_auth = core_auth
        self._state.params["core_host"] = core_host
        self._state.params["core_auth"] = core_auth
        # ====================

        self._state.service = CoreService(
            auth=core_auth, conn_url=core_host
        )

        self.update_state()

    def _result_collection_name(self, operation_id: str) -> str:
        return result_collection_name(operation_id)

    def _write_cache(self, object: object, name: str) -> None:
        cache.core.write_entry(
            json.dumps(object),
            entry_name=name,
            entry_group='interpreter/artifacts'
        )

    def _create_cfg_safe(
        self,
        auth: core.AUTH = None,
        conn_url: Optional[str] = None,
        **kwargs,
    ) -> None:
        auth = auth or self.__core_auth
        conn_url = conn_url or self.__core_host
        try:
            cfg_ = core.get_cfg(
                kwargs['cfg_id'],
                auth=auth,
                conn_url=conn_url
            ).id
        except Exception:
            core.create_cfg(
                **kwargs,
                auth=auth,
                conn_url=conn_url
            )
        else:
            core.update_cfg(
                cfg_,
                auth=auth,
                conn_url=conn_url,
                **kwargs
            )

    def interpret(self, node: TreeNode, component: ComponentSchema = None):  # noqa: ANN201
        return super().interpret(node, component)

    def create_node(
        self,
        state: CoreInterpreterState,
        tracer: traced[BaseNode],
    ) -> CoreInterpreterState:
        if isinstance(tracer.owner, OperationNode):
            if tracer.owner.alias is None:
                tracer.owner.alias = unique(tracer.owner.processor_id)
            state.operation_nodes[tracer.owner.alias] = tracer.owner

            extra = registry.get(
                tracer.owner.operation_id,
                model=CoreRegistryEntry
            )

            if not extra.image_ref:
                verbose_ = ""
                if tracer.owner.processor_id is not None:
                    verbose_ += f"processor: {tracer.owner.processor_id}, "
                if tracer.owner.package_id is not None:
                    verbose_ += f"package: {tracer.owner.package_id}."
                if not verbose_:
                    verbose_ = \
                        "The processor and the package cannot be determined " \
                        "(most probably, the app is installed " \
                        "with older version of Malevich)"
                raise InterpretationError(
                    "Found unknown operation. Possibly, you are using "
                    "an outdated version of the environment, or use are not in "
                    "the project directory (with malevich.yaml). \n"
                    + verbose_
                )

            if tracer.owner.is_condition:
                state.conditions[tracer.owner.alias] = Condition(
                    arguments={},
                    cfg=json.dumps({
                        **tracer.owner.config,
                        CORE_INTERPRETER_IN_APP_INFO_KEY: InjectedAppInfo(
                            conn_url=self.__core_host,
                            auth=self.__core_auth,
                            app_id=tracer.owner.alias, # NOTE: was processor_id
                            image_auth=(extra.image_auth_user, extra.image_auth_pass),
                            image_ref=extra.image_ref
                        ).model_dump()
                    }),
                    image=JsonImage(
                        ref=extra.image_ref,
                        user=extra.image_auth_user,
                        token=extra.image_auth_pass
                    ),
                    conditionId=tracer.owner.processor_id,
                )

            else:
                state.processors[tracer.owner.alias] = Processor(
                    arguments={},
                    cfg=json.dumps({
                        **tracer.owner.config,
                        CORE_INTERPRETER_IN_APP_INFO_KEY: InjectedAppInfo(
                            conn_url=self.__core_host,
                            auth=self.__core_auth,
                            app_id=tracer.owner.alias, # NOTE: was processor_id
                            image_auth=(extra.image_auth_user, extra.image_auth_pass),
                            image_ref=extra.image_ref
                        ).model_dump()
                    }),
                    image=JsonImage(
                        ref=extra.image_ref,
                        user=extra.image_auth_user,
                        token=extra.image_auth_pass
                    ),
                    processorId=tracer.owner.processor_id,
                )

            state.results[tracer.owner.alias] = [Result(name=tracer.owner.alias)]

        elif isinstance(tracer.owner, CollectionNode):
            if tracer.owner.alias is None:
                tracer.owner.alias = unique(tracer.owner.collection.collection_id)
            state.collection_nodes[tracer.owner.alias] = tracer.owner
        elif isinstance(tracer.owner, AssetNode):
            if tracer.owner.alias is None:
                tracer.owner.alias = unique(tracer.owner.name)
            state.asset_nodes[tracer.owner.alias] = tracer.owner
        elif isinstance(tracer.owner, TreeNode):
            # Cannot be the case, AbstractInterpreter
            # unwinds the tree is support_subtrees is False
            pass
        elif isinstance(tracer.owner, DocumentNode):
            if tracer.owner.alias is None:
                tracer.owner.alias = unique(tracer.owner.reverse_id)
            state.document_nodes[tracer.owner.alias] = tracer.owner

        _log(f"Node: {tracer.owner.uuid}, {tracer.owner.short_info()}", -1, 0, True)
        return state

    def _add_argument(
        self,
        state: CoreInterpreterState,
        argument: Argument,
        alias: str,
        link: ArgumentLink[BaseNode],
        is_condition: bool = False
    ):
        op_group = state.processors if not is_condition else state.conditions
        a = op_group[alias].arguments.get(link.name, None)
        if a is not None: # has previous arguments
            assert a.alternative or not argument.conditions, (
                "violation: argument with conditions, but no alternative"
                f" {a.model_dump()}"
            )
            if argument.conditions:
                a.alternative.append(argument)
            else:
                assert link.in_sink, (
                    "violation: clash of arguments but no in_sink flag"
                )

                if a.group is None:
                    a.group = []
                a.group.append(argument)
        else:
            if argument.conditions:
                a = AlternativeArgument(alternative=[argument])
            elif link.in_sink:
                a = AlternativeArgument(group=[argument])
            else:
                a = AlternativeArgument(**argument.model_dump())

        op_group[alias].arguments[link.name] = a


    def make_argument(
        self,
        state: CoreInterpreterState,
        node: BaseNode,
        conditions: dict[OperationNode, str] | None = None,
    ) -> Argument | AlternativeArgument:

        conditions = {
            key.alias: value
            for key, value in (conditions or {}).items()
        }

        if isinstance(node, CollectionNode):
            argument = Argument(
                collectionName=state.collection_nodes[node.alias].collection.collection_id,
                conditions=conditions
            )
        elif isinstance(node, OperationNode):
            argument = Argument(
                id=node.alias,
                indices=node.subindex,
                conditions=conditions
            )
        elif isinstance(node, AssetNode):
            argument = Argument(
                collectionName=state.asset_nodes[node.alias].name,
                conditions=conditions
            )
        elif isinstance(node, DocumentNode):
            argument = Argument(
                collectionName=state.document_nodes[node.alias].reverse_id,
                conditions=conditions
            )

        return argument

    def create_dependency(
        self,
        state: CoreInterpreterState,
        from_node: traced[BaseNode],
        to_node: traced[OperationNode],
        link: ArgumentLink[BaseNode],
        conditions: dict[str, bool] | None = None
    ) -> CoreInterpreterState:
        for fcondition, fnode in from_node.owner:
            for tcondition, tnode in to_node.owner:
                if fnode.alias is None:
                    for node in self._state.operation_nodes.values():
                        if node.uuid == fnode.uuid:
                            fnode.alias = node.alias
                            break
                    else:
                        fnode.alias = unique(fnode.uuid)

                cond_stmt = {
                    **(fcondition or {}),
                    **(conditions or {}),
                    **(tcondition or {})
                }
                self._add_argument(
                    state,
                    self.make_argument(state, fnode, cond_stmt),
                    tnode.alias,
                    link,
                    is_condition=(isinstance(tnode, OperationNode) and tnode.is_condition)
                )

                _log(
                    f"Dependency: {fnode.short_info()} -> "
                    f"{tnode.short_info()}, "
                    f"Link: {link.name}"
                    f"", -1, 0, True
                )
        return state

    def before_interpret(self, state: CoreInterpreterState) -> CoreInterpreterState:
        _log("Connection to Core is established.", 0, 0, True)
        _log(f"Core host: {self.__core_host}", 0, 0, True)
        try:
            core.check_auth(
                auth=self.__core_auth,
                conn_url=self.__core_host
            )
        except Exception:
            try:
                core.create_user(
                    auth=self.__core_auth,
                    conn_url=self.__core_host
                )
            except Exception:
                raise Exception(
                    "Cannot connect to Core. Please, check your credentials."
                )
        return state

    def after_interpret(self, state: CoreInterpreterState) -> CoreInterpreterState:
        for operation in state.operation_nodes.values():
            if operation.is_condition:
                continue
            if operation.should_be_true:
                for cond_uid in operation.should_be_true:
                    for condition in state.operation_nodes.values():
                        if condition.uuid == cond_uid:
                            _log(f"Condition {operation.alias}: {condition.alias} == True", -1, step=True)
                            if not state.processors[operation.alias].conditions:
                                state.processors[operation.alias].conditions = {}
                            state.processors[operation.alias].conditions[condition.alias] = True
            if operation.should_be_false:
                for cond_uid in operation.should_be_false:
                    for condition in state.operation_nodes.values():
                        if condition.uuid == cond_uid:
                            if not state.processors[operation.alias].conditions:
                                state.processors[operation.alias].conditions = {}
                            _log(f"Condition {operation.alias}: {condition.alias} == False", -1, step=True)
                            state.processors[operation.alias].conditions[condition.alias] = False

        return state

    def get_task(
        self, state: CoreInterpreterState
    ) -> CoreTask:
        """Creates an instance of task using interpreted state

        Args:
            state (CoreInterpreterV2State): State of the interpreter

        Returns:
            CoreTask: An instance of task
        """
        _log("Task is being compiled...")


        return CoreTask(
            state=state,
            component=self._component,
        )

    def attach(
        self,
        unique_task_hash: str,
        only_fetch: bool = False,
    ) -> CoreTask:
        task = CoreTask(self.state)
        return task.connect(
            unique_task_hash=unique_task_hash,
            only_fetch=only_fetch
        )
