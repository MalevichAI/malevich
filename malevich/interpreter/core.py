import enum
import json
import os
import uuid
from collections import defaultdict
from copy import deepcopy
from typing import Any, Iterable, Optional

import malevich_coretools as core
import pandas as pd

from .._autoflow.tracer import traced
from .._core.ops import (
    _assure_asset,
    batch_create_apps,
    batch_create_tasks,
    batch_upload_collections,
    result_collection_name,
)
from .._utility.cache.manager import CacheManager
from .._utility.logging import LogLevel, cout
from .._utility.registry import Registry
from ..constants import DEFAULT_CORE_HOST
from ..interpreter.abstract import Interpreter
from ..manifest import ManifestManager
from ..models.actions import Action
from ..models.argument import ArgumentLink
from ..models.exceptions import InterpretationError
from ..models.injections import BaseInjectable
from ..models.nodes import BaseNode, CollectionNode, OperationNode
from ..models.nodes.asset import AssetNode
from ..models.nodes.tree import TreeNode
from ..models.preferences import VerbosityLevel
from ..models.registry.core_entry import CoreRegistryEntry
from ..models.results.core.result import CoreLocalDFResult, CoreResult
from ..models.task.interpreted import InterpretedTask

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


class PrepareStages(enum.Enum):
    BUILD = 0b01
    BOOT = 0b10
    ALL = 0b11


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
        self.depends: dict[str, list[tuple[BaseNode, ArgumentLink]]] = defaultdict(lambda: [])  # noqa: E501
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
        self.extra_colls: dict[str, dict[str, str]] = defaultdict(lambda: {})


class CoreInjectable(BaseInjectable[str, str]):
    def __init__(
        self,
        collection_id: str,
        alias: str,
        uploaded_id: Optional[str] = None,
    ) -> None:
        self.__collection_id = collection_id
        self.__alias = alias
        self.__uploaded_id = uploaded_id

    def get_inject_data(self) -> str:
        return self.__collection_id

    def get_inject_key(self) -> str:
        return self.__alias

    def get_uploaded_id(self) -> Optional[str]:
        return self.__uploaded_id


core.task_run

class CoreInterpreter(Interpreter[CoreInterpreterState, tuple[str, str]]):
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
    set :code:`platform` and :code:`platformSettings` parameters in the
    :code:`.configure()` method. The :code:`platform` parameter can be
    :code:`base` or :code:`vast`. The :code:`platformSettings` parameter
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

    def _write_cache(self, object: object, path: str) -> None:
        json.dump(object, cache.get_file(os.path.join('core', path), 'w+'))

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
        _log(f"Core host: {self.__core_host}", 0, -1, True)
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

            app_core_name = op.uuid + \
                f"-{extra['processor_id']}-{op.alias}"

            if not op.alias:
                op.alias = extra["processor_id"] + '-' + \
                    str(_name(extra["processor_id"]))

            state.core_ops[op.uuid] = app_core_name
            state.app_args[op.uuid] = {
                'app_id': app_core_name,
                'extra': extra,
                'image_auth': (image_auth_user, image_auth_pass),
                'image_ref': image_ref,
                'app_cfg': op.config,
                'uid': op.uuid,
                'platform': 'base'
            }
            state.results[op.uuid] = self._result_collection_name(op.uuid)

        _log("Uploading to Core is completed.", step=True)
        return state

    def get_task(
        self, state: CoreInterpreterState
    ) -> InterpretedTask[CoreInterpreterState]:
        """Creates an instance of task using interpreted state

        Args:
            state (CoreInterpreterState): State of the interpreter

        Returns:
            InterpretedTask[CoreInterpreterState]: An instance of task
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

        self._create_cfg_safe(**config_kwargs)
        # ____________________________________________

        # NOTE: New core representation should affect this line
        # Extracting the only leave to supply it to task run
        leaves = [*self._tree.leaves()]

        # ____________________________________________
        # Now, callbacks are declared. Callbacks are then available
        # from PromisedTask interface. That way of function declaration
        # allows to capture external variables and pass them to callbacks
        # without any additional effort

        # * Configure: allows to configure the platform for the app
        # ==========================================
        def configure(
            # Instance of the task (self)
            task: InterpretedTask[CoreInterpreterState],
            # Operation identifier (alias)
            operation: str,
            # Configurable parameters
            platform: str = 'base',
            platformSettings: Optional[dict[str, Any]] = None,  # noqa: N803
            # Rest of the parameters for compatibility
            **kwargs
        ) -> None:
            assert platform in ['base', 'vast'], \
                f"Platform {platform} is not supported. "

            uuid_ = {
                k.alias: k.uuid for k in state.ops.values()
            }[operation]

            state.app_args[uuid_]['platform'] = platform
            if platformSettings:
                state.app_args[uuid_]['platformSettings'] = platformSettings

        # ========================================== (configure)

        # * Prepare: prepares the task for execution
        # ==========================================
        def prepare(
            task: InterpretedTask[CoreInterpreterState],
            stage: PrepareStages = PrepareStages.ALL,
            *args,
            **kwargs
        ) -> None:
            _log(
                "Task is being prepared for execution. It may take a while",
                action=1
            )
            if stage.value & PrepareStages.BUILD.value:
                apps_ = batch_create_apps([
                    {
                        'auth': state.params["core_auth"],
                        'conn_url': state.params["core_host"],
                        **_app_args,
                        'extra_collections_from': state.extra_colls[_app_args['uid']]
                    } for _app_args in state.app_args.values()
                ])

                for settings, arg_ in apps_:
                    task.state.cfg.app_settings.append(settings)
                    self._write_cache(
                        arg_,
                        f"app-{settings.appId}-{self.state.interpretation_id}.json"
                    )
                # for _app_args in state.app_args.values():
                #     task.state.cfg.app_settings.append(
                #         self._create_app_safe(
                #             **_app_args,
                #             extra_collections_from=state.extra_colls[_app_args['uid']]
                #         )
                #     )

                batch_create_tasks(
                    task_kwargs,
                    auth=state.params["core_auth"],
                    conn_url=state.params["core_host"]
                )
                # for _kwargs in task_kwargs:
                #     core.create_task(**_kwargs, wait=False)

                self._create_cfg_safe(**config_kwargs)
                task.state.params['task_id'] = task.state.core_ops[leaves[0].owner.uuid]

            if stage.value & PrepareStages.BOOT.value:
                try:
                    task.state.params["operation_id"] = core.task_prepare(
                        task_id=task.state.params['task_id'],
                        cfg_id=config_kwargs['cfg_id'],
                        auth=task.state.params["core_auth"],
                        conn_url=task.state.params["core_host"],
                        *args,
                        **kwargs
                    ).operationId
                except (Exception, KeyboardInterrupt) as e:
                    # Cleanup
                    core.task_stop(
                        task.state.params['task_id'],
                        auth=task.state.params["core_auth"],
                        conn_url=task.state.params["core_host"],
                    )
                    raise e
        # ========================================== (prepare)

        # * Run: runs the task
        # ==========================================
        def run(
            task: InterpretedTask[CoreInterpreterState],
            overrides: Optional[dict[str, str]] = None,
            run_id: Optional[str] = None,
            *args,
            **kwargs
        ) -> None:
            _log("Task is being executed on Core. It may take a while", action=2)
            if "operation_id" not in task.state.params:
                raise Exception("Attempt to run a task which is not prepared. "
                                "Please, run `.prepare()` first.")

            _cfg = deepcopy(task.state.cfg)
            if run_id:
                _cfg.app_settings = [
                    core.AppSettings(
                        taskId=s.taskId,
                        appId=s.appId,
                        saveCollectionsName=(s.saveCollectionsName if isinstance(
                            s.saveCollectionsName, str) else s.saveCollectionsName[0]
                        ) + '_' + str(run_id)
                    ) for s in _cfg.app_settings
                ]
            try:
                if overrides:
                    _cfg.collections = {
                        **_cfg.collections,
                        **overrides,
                    }
                    _cfg_id = config_kwargs['cfg_id'] + \
                        '-overridden' + uuid.uuid4().hex
                    self._create_cfg_safe(
                        cfg_id=_cfg_id,
                        cfg=_cfg,
                        conn_url=task.state.params["core_host"],
                        auth=task.state.params["core_auth"],
                    )

                    core.task_run(
                        task.state.params["operation_id"],
                        cfg_id=_cfg_id,
                        auth=task.state.params["core_auth"],
                        conn_url=task.state.params["core_host"],
                        *args,
                        **kwargs
                    )
                else:
                    core.task_run(
                        task.state.params["operation_id"],
                        *args,
                        auth=task.state.params["core_auth"],
                        conn_url=task.state.params["core_host"],
                        **kwargs
                    )
            except (Exception, KeyboardInterrupt) as e:
                # Cleanup
                core.task_stop(
                    task.state.params["operation_id"],
                    auth=task.state.params["core_auth"],
                    conn_url=task.state.params["core_host"],
                )
                raise e

        # ========================================== (run)

        # * Stop: stops the task
        # ==========================================
        def stop(
            task: InterpretedTask[CoreInterpreterState],
            *args,
            **kwargs
        ) -> None:
            if "operation_id" not in task.state.params:
                raise Exception("Attempt to run a task which is not prepared. "
                                "Please, run `.prepare()` first.")
            core.task_stop(
                task.state.params["operation_id"],
                *args,
                auth=task.state.params["core_auth"],
                conn_url=task.state.params["core_host"],
                **kwargs
            )
        # ========================================== (stop)

        # * Results: fetches the results from Core for
        # requested nodes
        # ==========================================
        def results(
            task: InterpretedTask[CoreInterpreterState],
            returned: Iterable[traced[BaseNode]] | traced[BaseNode] | None,
            run_id: Optional[str] = None,
            *args,
            **kwargs
        ) -> Iterable[pd.DataFrame] | pd.DataFrame:
            _log("Task results are being fetched from Core", action=3)
            if "operation_id" not in task.state.params:
                raise Exception("Attempt to run a task which is not prepared. "
                                "Please, run `.prepare()` first.")
            return self.get_results(
                task_id=task.state.params['operation_id'],
                returned=returned,
                run_id=run_id
            )
        # ========================================== (results)

        # * Injectables: returns the list of points
        # which can be changed from run to run
        def injectables(
            task: InterpretedTask[CoreInterpreterState]
        ) -> list[BaseInjectable]:
            injectables = []
            nodes_ = set()
            nodes: Iterable[BaseNode] = []
            for x in task.state.ops.values():
                nodes.append(x)
                nodes_.add(x.uuid)

            for node in nodes:
                if isinstance(node, CollectionNode):
                    for cfg_coll_id, core_coll_id in state.cfg.collections.items():
                        if cfg_coll_id == node.collection.collection_id:
                            injectables.append(
                                CoreInjectable(
                                    collection_id=node.collection.collection_id,
                                    alias=node.alias,
                                    uploaded_id=core_coll_id
                                )
                            )

                            break

            return injectables
        # ========================================== (injectables)

        def ops(*args, **kwargs) -> list[str | None]:
            return [x.alias for x in state.ops.values() if isinstance(x, OperationNode)]

        return InterpretedTask(
            prepare=prepare,
            run=run,
            stop=stop,
            results=results,
            state=state,
            get_injectables=injectables,
            # TODO: Make it more robust
            get_operations=ops,
            configure=configure
        )

    def get_results(
        self,
        task_id: str,
        returned: Iterable[traced[BaseNode]] | traced[BaseNode] | None,
        run_id: Optional[str] = None,
    ) -> Iterable[pd.DataFrame]:
        if not returned:
            return None

        if isinstance(returned, traced):
            returned = [returned]

        results = []
        for r in returned:
            node = r.owner
            if isinstance(node, CollectionNode):
                results.append(
                    CoreLocalDFResult(
                        dfs=[node.collection.collection_data]
                    )
                )
            elif isinstance(node, OperationNode):
                results.append(CoreResult(
                    core_group_name=
                        self._result_collection_name(node.uuid) +   #group_name
                        (f'_{run_id}' if run_id else ''),           #group_name
                    core_operation_id=task_id,
                    auth=self.state.params["core_auth"],
                    conn_url=self.state.params["core_host"],
                ))
            elif isinstance(node, AssetNode):
                results.append(CoreResult(
                    core_group_name=
                        self._result_collection_name(node.uuid) +   #group_name
                        (f'_{run_id}' if run_id else ''),           #group_name
                    core_operation_id=task_id,
                    auth=self.state.params["core_auth"],
                    conn_url=self.state.params["core_host"],
                ))

            elif isinstance(node, TreeNode):
                results.extend(self.get_results(
                    task_id=task_id, returned=node.results, run_id=run_id
                )
            )
        # return results[0] if len(results) == 1 else results
        return results

