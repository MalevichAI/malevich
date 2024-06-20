import json
import uuid
from collections import defaultdict
from typing import Optional

import malevich_coretools as core
from malevich_coretools import Argument, JsonImage, Processor, Result
from malevich_space.schema import ComponentSchema

from malevich._utility import unique

from .._autoflow.tracer import traced, tracedLike
from .._core.ops import (
    _assure_asset,
    batch_upload_collections,
    result_collection_name,
)
from .._utility.cache.manager import CacheManager
from .._utility.logging import LogLevel, cout
from .._utility.registry import Registry
from ..constants import CORE_INTERPRETER_IN_APP_INFO_KEY, DEFAULT_CORE_HOST
from ..interpreter.abstract import Interpreter
from ..models.actions import Action
from ..models.argument import ArgumentLink
from ..models.exceptions import InterpretationError
from ..models.in_app_core_info import InjectedAppInfo
from ..models.nodes import BaseNode, CollectionNode, OperationNode
from ..models.nodes.asset import AssetNode
from ..models.nodes.tree import TreeNode
from ..models.preferences import VerbosityLevel
from ..models.registry.core_entry import CoreRegistryEntry
from ..models.state.core_v2 import CoreInterpreterV2State
from ..models.task.interpreted.core import CoreTask
from ..models.task.interpreted.core_v2 import CoreTaskV2

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



class CoreInterpreterV2(Interpreter[CoreInterpreterV2State, CoreTask]):
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
        super().__init__(CoreInterpreterV2State())
        self.__core_host = core_host
        self.__core_auth = core_auth
        self._state.params["core_host"] = core_host
        self._state.params["core_auth"] = core_auth
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
        state: CoreInterpreterV2State,
        node: traced[BaseNode],
    ) -> CoreInterpreterV2State:
        if isinstance(node.owner, OperationNode):
            if node.owner.alias is None:
                node.owner.alias = unique.unique(node.owner.processor_id)
            state.operation_nodes[node.owner.alias] = node.owner

            extra = registry.get(
                node.owner.operation_id,
                model=CoreRegistryEntry
            )

            if not extra.image_ref:
                verbose_ = ""
                if node.owner.processor_id is not None:
                    verbose_ += f"processor: {node.owner.processor_id}, "
                if node.owner.package_id is not None:
                    verbose_ += f"package: {node.owner.package_id}."
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

            state.processors[node.owner.alias] = Processor(
                arguments={},
                cfg=json.dumps({
                    **node.owner.config,
                    CORE_INTERPRETER_IN_APP_INFO_KEY: InjectedAppInfo(
                        conn_url=self.__core_host,
                        auth=self.__core_auth,
                        app_id=node.owner.alias, # NOTE: was processor_id
                        image_auth=(extra.image_auth_user, extra.image_auth_pass),
                        image_ref=extra.image_ref
                    ).model_dump()
                }),
                image=JsonImage(
                    ref=extra.image_ref,
                    user=extra.image_auth_user,
                    token=extra.image_auth_pass
                ),
                processorId=node.owner.processor_id,
            )

            state.results[node.owner.alias] = [Result(name=node.owner.alias)]

        elif isinstance(node.owner, CollectionNode):
            if node.owner.alias is None:
                node.owner.alias = unique.unique(node.owner.collection.collection_id)
            state.collection_nodes[node.owner.alias] = node.owner
        elif isinstance(node.owner, AssetNode):
            if node.owner.alias is None:
                node.owner.alias = unique.unique(node.owner.core_path)
            state.asset_nodes[node.owner.alias] = node.owner
        elif isinstance(node.owner, TreeNode):
            # Cannot be the case, AbstractInterpreter
            # unwinds the tree is support_subtrees is False
            pass

        _log(f"Node: {node.owner.uuid}, {node.owner.short_info()}", -1, 0, True)
        return state

    def create_dependency(
        self,
        state: CoreInterpreterV2State,
        callee: traced[BaseNode],
        caller: traced[OperationNode],
        link: ArgumentLink[BaseNode],
    ) -> CoreInterpreterV2State:
        if isinstance(callee.owner, CollectionNode):
            state.processors[caller.owner.alias].arguments[link.name] = Argument(
                collectionName=state.collection_nodes[callee.owner.alias].collection.collection_id
            )
        elif isinstance(callee.owner, OperationNode):
            state.processors[caller.owner.alias].arguments[link.name] = Argument(
                id=callee.owner.alias,
                indices=callee.owner.subindex
            )
        elif isinstance(callee.owner, AssetNode):
            state.processors[caller.owner.alias].arguments[link.name] = Argument(
                collectionName=state.asset_nodes[callee.owner.alias].get_core_path()
            )

        _log(
            f"Dependency: {callee.owner.short_info()} -> {caller.owner.short_info()}, "
            f"Link: {link.name}", -1, 0, True
        )
        return state

    def before_interpret(self, state: CoreInterpreterV2State) -> CoreInterpreterV2State:
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

    def after_interpret(self, state: CoreInterpreterV2State) -> CoreInterpreterV2State:
        _log("Flow is built. Uploading to Core", step=True)
        return state

    def get_task(
        self, state: CoreInterpreterV2State
    ) -> CoreTask:
        """Creates an instance of task using interpreted state

        Args:
            state (CoreInterpreterV2State): State of the interpreter

        Returns:
            CoreTask: An instance of task
        """
        _log("Task is being compiled...")


        return CoreTaskV2(
            state=state,
            component=self._component,
        )

    def attach(self, unique_task_hash: str) -> CoreTaskV2:
        try:
            pipeline = core.get_pipeline(
                id=unique_task_hash,
                conn_url=self.__core_host,
                auth=self.__core_auth
            )

        except Exception:
            raise Exception("No pipeline found with id " + unique_task_hash)

        task = CoreTaskV2(self.state)

        task.state.processors = pipeline.processors
        task.state.results = pipeline.results
        task.state.conditions = pipeline.conditions
        task.state.unique_task_hash = unique_task_hash
        task.state.config = core.Cfg()

        json_cfg = json.loads(core.get_cfg(
            unique_task_hash,
            conn_url=self.__core_host,
            auth=self.__core_auth
        ).data)

        for key, value in json_cfg.items():
            setattr(task.state.config, key, value)

        node_results = [
            tracedLike(OperationNode(alias=x, operation_id=''))
            for x in pipeline.results.keys()
        ]
        task.commit_returned(node_results)
        return task
