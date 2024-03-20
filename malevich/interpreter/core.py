import json
import uuid
from collections import defaultdict
from typing import Optional

import malevich_coretools as core
from malevich_space.schema import ComponentSchema

from .._autoflow.tracer import traced
from .._core.ops import (
    _assure_asset,
    batch_upload_collections,
    result_collection_name,
)
from .._utility.cache.manager import CacheManager
from .._utility.logging import LogLevel, cout
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
from ..models.state.core import CoreInterpreterState
from ..models.task.interpreted.core import CoreTask

cache = CacheManager()

_levels = [LogLevel.Info, LogLevel.Warning, LogLevel.Error, LogLevel.Debug]
_actions = [Action.Interpretation,
            Action.Preparation, Action.Run, Action.Results]


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

    def __init__(
        self,
        core_auth: tuple[str, str],
        core_host: str = DEFAULT_CORE_HOST,
    ) -> None:
        super().__init__(CoreInterpreterState())
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
        self, state: CoreInterpreterState, node: traced[BaseNode]
    ) -> CoreInterpreterState:
        state.ops[node.owner.uuid] = node.owner
        _log(
            f"Node: {node.owner.uuid}, {node.owner.short_info()}", -1, 0, True)
        return state

    def create_dependency(
        self,
        state: CoreInterpreterState,
        callee: traced[BaseNode],
        caller: traced[BaseNode],
        link: ArgumentLink,
    ) -> CoreInterpreterState:
        state.depends[caller.owner.uuid].append((callee.owner, link))
        _log(
            f"Dependency: {caller.owner.short_info()} -> {callee.owner.short_info()}, "
            f"Link: {link.name}", -1, 0, True
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
        _log("Flow is built. Uploading to Core", step=True)

        collection_nodes = [
            x for x in state.ops.values() if isinstance(x, CollectionNode)
        ]

        asset_nodes = [
            x for x in state.ops.values() if isinstance(x, AssetNode)
        ]

        core_ids = batch_upload_collections(
            [x.collection for x in collection_nodes],
            conn_url=self.__core_host,
            auth=self.__core_auth
        )

        for node, core_id in zip(collection_nodes, core_ids):
            state.collections[node.uuid] = (
                node.collection.collection_id, core_id)
            if not node.alias:
                node.alias = node.collection.collection_id + '-' + \
                    str(_name(node.collection.collection_id))

        for node in asset_nodes:
            if isinstance(node, AssetNode):
                _assure_asset(node, self.__core_auth, self.__core_host)
                if not node.alias:
                    node.alias = node.core_path + '-' + \
                        str(_name(node.core_path))
                state.collections[node.uuid] = (
                    node.core_path, node.get_core_path())

        order_ = {
            **{k: v for k, v in state.ops.items() if isinstance(v, OperationNode)}
        }

        for id, op in order_.items():
            extra = state.reg.get(
                op.operation_id,
                {},
                model=CoreRegistryEntry
            )
            image_auth_user = extra.image_auth_user
            image_auth_pass = extra.image_auth_pass
            image_ref = extra.image_ref

            if not image_ref:
                verbose_ = ""
                if op.processor_id is not None:
                    verbose_ += f"processor: {op.processor_id}, "
                if op.package_id is not None:
                    verbose_ += f"package: {op.package_id}."
                if not verbose_:
                    verbose_ = \
                        "The processor and the package cannot be determined " \
                        "(most probably, the app is installed " \
                        "with older version of Malevich)"
                raise InterpretationError(
                    "Found unknown operation. Possibly, you are using "
                    "an outdated version of the environment.\n"
                    + verbose_
                )

            for node, link in state.depends[id]:
                if not (
                    type(node) in [CollectionNode, AssetNode]
                    and node.uuid in state.collections
                ):
                    continue

                coll, uploaded_core_id = state.collections[node.uuid]
                state.cfg.collections = {
                    **state.cfg.collections,
                    f"{coll}": uploaded_core_id,
                }
                state.extra_colls[op.uuid][link.name] = coll

            if not op.alias:
                op.alias = extra["processor_id"] + '-' + str(_name(extra["processor_id"]))  # noqa: E501

            app_core_name = op.uuid + f"-{extra['processor_id']}-{op.alias}"

            state.task_aliases[op.alias] = app_core_name
            state.core_ops[op.uuid] = app_core_name
            state.app_args[op.uuid] = {
                'app_id': app_core_name,
                'extra': extra,
                'image_auth': (image_auth_user, image_auth_pass),
                'image_ref': image_ref,
                'app_cfg': {
                    **op.config,
                    CORE_INTERPRETER_IN_APP_INFO_KEY: InjectedAppInfo(
                        conn_url=self.__core_host,
                        auth=self.__core_auth,
                        app_id=app_core_name,
                        image_auth=(image_auth_user, image_auth_pass),
                        image_ref=image_ref
                    ).model_dump()
                },
                'uid': op.uuid,
                'platform': 'base',
                'alias': op.alias
            }
            state.results[op.uuid] = result_collection_name(op.uuid, op.alias)

        _log("Uploading to Core is completed.", step=True)
        return state

    def get_task(
        self, state: CoreInterpreterState
    ) -> CoreTask:
        """Creates an instance of task using interpreted state

        Args:
            state (CoreInterpreterState): State of the interpreter

        Returns:
            CoreTask: An instance of task
        """
        _log("Task is being compiled...")
        task_kwargs = []
        config_kwargs = []

        # ____________________________________________

        # Firstly, using collected edges, lists of
        # dependencies for each task are created for each
        # of the apps.

        for id, core_id in sorted(
            state.core_ops.items(), key=lambda x: len(state.depends[x[0]])
        ):
            depends = state.depends[id]
            depends.sort(key=lambda x: x[1].index)
            depends = [
                state.core_ops[x[0].uuid]
                for x in depends
                if x[0].uuid in state.core_ops
            ]

            task_kwargs.append(
                {
                    'task_id': core_id,
                    'app_id': core_id,
                    'tasks_depends': depends,
                    # 'auth': state.params["core_auth"],
                    # 'conn_url': state.params["core_host"],
                }
            )

            self._write_cache(
                task_kwargs[-1],
                f'task-{core_id}-{self.state.interpretation_id}.json'
            )

        # ____________________________________________

        # Then, the base configuration is uploaded to the Core
        # using generated config ID. This configuration
        # is then used as a basis once overloaded on runs

        __cfg = uuid.uuid4().hex
        config_kwargs = {
            'cfg_id': __cfg,
            'cfg': state.cfg,
        }

        # Base configuration is preserved within the state
        state.params.base_config_id = __cfg
        state.params.base_config = state.cfg

        # self._create_cfg_safe(**config_kwargs)
        # ____________________________________________

        # NOTE: New core representation should affect this line
        # Extracting the only leave to supply it to task run
        leaves = [*self._tree.leaves()]

        return CoreTask(
            state=state,
            task_kwargs=task_kwargs,
            config_kwargs=config_kwargs,
            leaf_node_uid=leaves[0].owner.uuid,
            component=self._component,
        )

    # def get_results(
    #     self,
    #     task_id: str,
    #     returned: Iterable[traced[BaseNode]] | traced[BaseNode] | None,
    #     run_id: Optional[str] = None,
    # ) -> Iterable[pd.DataFrame]:
    #     if not returned:
    #         return None

    #     if isinstance(returned, traced):
    #         returned = [returned]

    #     results = []
    #     for r in returned:
    #         node = r.owner
    #         if isinstance(node, CollectionNode):
    #             results.append(
    #                 CoreLocalDFResult(
    #                     dfs=[node.collection.collection_data]
    #                 )
    #             )
    #         elif isinstance(node, OperationNode):
    #             results.append(CoreResult(
    #                 core_group_name=
    #                     self._result_collection_name(node.uuid) +   #group_name
    #                     (f'_{run_id}' if run_id else ''),           #group_name
    #                 core_operation_id=task_id,
    #                 auth=self.state.params["core_auth"],
    #                 conn_url=self.state.params["core_host"],
    #             ))
    #         elif isinstance(node, AssetNode):
    #             results.append(CoreResult(
    #                 core_group_name=
    #                     self._result_collection_name(node.uuid) +   #group_name
    #                     (f'_{run_id}' if run_id else ''),           #group_name
    #                 core_operation_id=task_id,
    #                 auth=self.state.params["core_auth"],
    #                 conn_url=self.state.params["core_host"],
    #             ))

    #         elif isinstance(node, TreeNode):
    #             results.extend(self.get_results(
    #                 task_id=task_id, returned=node.results, run_id=run_id
    #             )
    #         )
    #     # return results[0] if len(results) == 1 else results
    #     return results

