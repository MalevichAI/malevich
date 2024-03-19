import enum
import pickle
import uuid
import warnings
from copy import deepcopy
from typing import Any, Iterable, Literal, Optional, Type

import malevich_coretools as core
import pandas as pd
from malevich_space.schema import ComponentSchema

from malevich.models.endpoint import MetaEndpoint
from malevich.models.types import FlowOutput

from ...._autoflow.tracer import traced
from ...._core.ops import (
    batch_create_apps,
    batch_create_tasks,
    batch_upload_collections,
)
from ...._utility.logging import LogLevel, cout
from ....interpreter.abstract import Interpreter
from ...actions import Action
from ...collection import Collection
from ...injections import CoreInjectable
from ...nodes.asset import AssetNode
from ...nodes.base import BaseNode
from ...nodes.collection import CollectionNode
from ...nodes.operation import OperationNode
from ...nodes.tree import TreeNode
from ...preferences import VerbosityLevel
from ...results.core.result import CoreLocalDFResult, CoreResult
from ...state.core import CoreInterpreterState
from ..base import BaseTask


class PrepareStages(enum.Enum):
    BUILD = 0b01
    BOOT = 0b10
    ALL = 0b11


class CoreTaskStage(enum.Enum):
    ONLINE = 0
    BUILT = 1
    NO_TASK = 3


class CoreTask(BaseTask):
    """Represents a task on Malevich Core.

    Provides a user-friendly interfaces to interact with the task
    using Malevich Core API.
    """

    @staticmethod
    def load(object_bytes: bytes) -> 'CoreTask':
        return pickle.loads(object_bytes)

    def __init__(
        self,
        state: CoreInterpreterState,
        task_kwargs: list[dict] = None,  # noqa: RUF013
        config_kwargs: dict[str, Any] = None,  # noqa: RUF013
        leaf_node_uid: str = None,  # noqa: RUF013
        partial: bool = False,
        component: ComponentSchema | None = None
    ) -> None:
        super().__init__()
        if state is None:
            raise Exception("CoreTask requires a self.state to be passed. ")

        self.state = state
        self.task_kwargs = task_kwargs
        self.config_kwargs = config_kwargs
        self.leaf_node_uid = leaf_node_uid
        self._partial = partial
        self._returned = None
        self.run_id = None
        self.component = component

    def _create_cfg_safe(
        self,
        auth: core.AUTH = None,
        conn_url: Optional[str] = None,
        **kwargs,
    ) -> None:
        auth = auth or self.state.params.core_auth
        conn_url = conn_url or self.state.params.core_host
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

    def get_stage(self) -> CoreTaskStage:
        if self.state.params.operation_id is not None:
            try:
                runs = core.get_run_active_runs(
                    auth=self.state.params.core_auth,
                    conn_url=self.state.params.core_host,
                ).ids
                if self.state.params.operation_id in runs:
                    return CoreTaskStage.ONLINE
            except Exception:
                pass

        if self.state.params.task_id is not None:
            try:
                core.get_task(
                    self.state.params.task_id,
                    auth=self.state.params.core_auth,
                    conn_url=self.state.params.core_host
                )
                return CoreTaskStage.BUILT
            except Exception:
                pass

        return CoreTaskStage.NO_TASK

    def get_stage_class(self) -> Type[CoreTaskStage]:
        pass

    def configure(
            self,
            operation: str,
            # Configurable parameters
            platform: str = 'base',
            platform_settings: Optional[dict[str, Any]] = None,
            # Rest of the parameters for compatibility
            **kwargs
        ) -> None:
        """Configures the operation on Malevich Core

        Available configurations
        ------------------------

        Platform
        ++++++++

        The platform determines the environment in which the operation will be executed.
        We support two platforms: `base` and `vast`. The `base` platform is the default
        Malevich Core cluster. The `vast` platform is a on-demand GPU cluster that can
        be utilized for GPU-intensive operations.

        To configure the platform, use the `platform` parameter. The default value is
        `base`. To use the `vast` platform, set the value to `vast` and configure
        it with `malevich.core_api.vast_settings`.
        """
        assert platform in [
            'base', 'vast'], f"Platform {platform} is not supported. "

        uuid_ = {
            k.alias: k.uuid
            for k in self.state.ops.values()
        }[operation]

        self.state.app_args[uuid_]['platform'] = platform
        if platform_settings:
            self.state.app_args[uuid_]['platform_settings'] = platform_settings

    def prepare(
        self,
        stage: PrepareStages = PrepareStages.ALL,
        *args,
        **kwargs
    ) -> None:
        """Prepares the task to be executed on Malevich Core

        The method is divided into two stages: build and boot.
        During build stage all the necessary components are send
        to the Core. During boot stage the task is actually
        deployed and gets ready for accepting runs.

        Args:
            - stage (PrepareStages, optional): The stage to be executed.
                Defaults to PrepareStages.ALL.
            - *args (Any, optional):
                Positional arguments to be passed to the :func`malevich.core_api.task_prepare`
                function.
            - **kwargs (Any, optional):
                Keyword arguments to be passed to the :func`malevich.core_api.task_prepare`
                function.

        Returns:
            tuple[str, str]: The task and operation IDs within Malevich Core. Some
                of them may be None if stage is specified.
        """  # noqa: E501
        cout(
            action=Action.Preparation,
            message="Task is being prepared for execution. It may take a while",
            verbosity=VerbosityLevel.OnlyStatus,
            level=LogLevel.Info
        )

        if stage.value & PrepareStages.BUILD.value:
            apps_ = batch_create_apps([
                {
                    **_app_args,
                    'auth': self.state.params.core_auth,
                    'conn_url': self.state.params.core_host,
                    'extra_collections_from': self.state.extra_colls[_app_args['uid']]
                } for _app_args in self.state.app_args.values()
            ])

            for settings, _ in apps_:
                self.state.cfg.app_settings.append(settings)
            self.config_kwargs['cfg'] = self.state.cfg
                # TODO: Write cache
            batch_create_tasks(
                self.task_kwargs,
                auth=self.state.params.core_auth,
                conn_url=self.state.params.core_host
            )

            self._create_cfg_safe(**self.config_kwargs)
            self.state.params.task_id = self.state.core_ops[self.leaf_node_uid]

        if stage.value & PrepareStages.BOOT.value:
            try:
                self.state.params.operation_id = core.task_prepare(
                    task_id=self.state.params.task_id,
                    cfg_id=self.config_kwargs['cfg_id'],
                    auth=self.state.params.core_auth,
                    conn_url=self.state.params.core_host,
                    *args,
                    **kwargs
                ).operationId
            except (Exception, KeyboardInterrupt) as e:
                # Cleanup
                core.task_stop(
                    self.state.params.task_id,
                    auth=self.state.params.core_auth,
                    conn_url=self.state.params.core_host,
                )
                raise e

        return self.state.params.task_id, self.state.params.operation_id

    def run(
        self,
        override: dict[str, pd.DataFrame] | None = None,
        run_id: Optional[str] = None,
        detached: bool = False,
        *args,
        **kwargs
    ) -> str:
        """Runs the task on Malevich Core

        Overrides is a dictionary of collection overrides within Malevich Core
        in the following form:

        .. code-block:: json

            {
                "collection_name": "new_collection_id"
                // ...
            }

        Args:
            - overrides (dict[str, str], optional):
                Collection overrides within Malevich Core.
                The dict in the form:
            - run_id (str, optional): The ID of the run. Defaults to None.
            - detached (bool, optional): Whether to wait for the task to finish. Defaults
                to False.
            - *args (Any, optional): Positional arguments to be passed to the
                :func:`malevich.core_api.task_run` function.
            - **kwargs (Any, optional): Keyword arguments to be passed to the
                :func:`malevich.core_api.task_run` function.

        """  # noqa: E501
        cout(
            message="Task is being executed on Core. It may take a while",
            action=Action.Run,
            verbosity=VerbosityLevel.OnlyStatus,
        )

        if "operation_id" not in self.state.params:
            raise Exception("Attempt to run a task which is not prepared. "
                            "Please, run `.prepare()` first.")

        if override:
            collections = [
                Collection(
                    collection_id=f'core-interpreter-override-{k}-{run_id}',
                    collection_data=v,
                    persistent=False
                ) for k, v in override.items()
            ]

            core_ids = batch_upload_collections(
                collections,
                conn_url=self.state.params.core_host,
                auth=self.state.params.core_auth,
            )

            injectables = self.get_injectables()
            key_to_core_id = {
                k: core_id
                for k, core_id in zip(override.keys(), core_ids)
            }

            real_overrides = {
                injectable.get_inject_data():
                key_to_core_id[injectable.get_inject_key()]
                for injectable in injectables
            }
        else:
            real_overrides = {}

        _cfg = deepcopy(self.state.cfg)

        self.run_id = run_id or uuid.uuid4().hex
        _cfg.app_settings = [
            core.AppSettings(
                taskId=s.taskId,
                appId=s.appId,
                saveCollectionsName=(s.saveCollectionsName if isinstance(
                    s.saveCollectionsName, str) else s.saveCollectionsName[0]
                )
            ) for s in _cfg.app_settings
        ]

        try:
            if real_overrides:
                _cfg.collections = {
                    **_cfg.collections,
                    **real_overrides,
                }
                _cfg_id = self.config_kwargs['cfg_id'] + \
                    '-overridden' + uuid.uuid4().hex
                self._create_cfg_safe(
                    cfg_id=_cfg_id,
                    cfg=_cfg,
                    conn_url=self.state.params.core_host,
                    auth=self.state.params.core_auth,
                )
                core.task_run(
                    self.state.params.operation_id,
                    cfg_id=_cfg_id,
                    auth=self.state.params.core_auth,
                    conn_url=self.state.params.core_host,
                    wait=not detached,
                    run_id=self.run_id,
                    *args,
                    **kwargs
                )
            else:
                core.task_run(
                    self.state.params.operation_id,
                    *args,
                    auth=self.state.params.core_auth,
                    conn_url=self.state.params.core_host,
                    run_id=self.run_id,
                    wait=not detached,
                    **kwargs
                )
        except (Exception, KeyboardInterrupt) as e:
            # Cleanup
            core.task_stop(
                self.state.params.operation_id,
                auth=self.state.params.core_auth,
                conn_url=self.state.params.core_host,
            )
            raise e
        return self.run_id

    def stop(
        self,
        *args,
        **kwargs
    ) -> None:
        """Stops the task preventing it from further execution

        Args:
            - *args (Any, optional): Positional arguments to be passed to the
                :func:`malevich.core_api.task_stop` function.
            - **kwargs (Any, optional): Keyword arguments to be passed to the
                :func:`malevich.core_api.task_stop` function.
        """
        if "operation_id" not in self.state.params:
            raise Exception("Attempt to run a task which is not prepared. "
                            "Please, run `.prepare()` first.")
        core.task_stop(
            self.state.params.operation_id,
            *args,
            auth=self.state.params.core_auth,
            conn_url=self.state.params.core_host,
            **kwargs
        )

    def results(
        self,
        # returned: Iterable[traced[BaseNode]] | traced[BaseNode] | None,
        run_id: Optional[str] = None,
        # For compatibility with other interpreters
        *args,
        **kwargs
    ) -> Iterable[CoreResult | CoreLocalDFResult]:
        cout(message="Task results are being fetched from Core",
             action=Action.Results)
        if self.state.params.operation_id is None:
            raise Exception("Attempt to run a task which is not prepared. "
                            "Please, run `.prepare()` first.")

        returned = self._returned
        if not self._returned:
            return None

        if not run_id:
            run_id = self.run_id

        if isinstance(self._returned, traced):
            returned = [returned]

        def _deflat(li: list[traced[BaseNode]]) -> list[traced[BaseNode]]:
            temp_returned = []
            for r in li:
                if isinstance(r.owner, TreeNode):
                    temp_returned.extend(
                        _deflat(r.owner.results)
                    )
                else:
                    temp_returned.append(r)
            return temp_returned

        returned = _deflat(returned)

        results = []
        for r in returned:
            node = r.owner
            if isinstance(node, CollectionNode):
                results.append(
                    CoreLocalDFResult(
                        coll=node.collection,
                        conn_url=self.state.params.core_host,
                        auth=self.state.params.core_auth,
                    )
                )
            elif isinstance(node, OperationNode):
                results.append(CoreResult(
                    core_group_name=self.state.results[node.uuid],
                    core_operation_id=self.state.params.operation_id,
                    core_run_id=run_id,
                    auth=self.state.params.core_auth,
                    conn_url=self.state.params.core_host,
                ))
            elif isinstance(node, AssetNode):
                results.append(CoreResult(
                    core_group_name=self.state.results[node.uuid],
                    core_operation_id=self.state.params.operation_id,
                    core_run_id=run_id,
                    conn_url=self.state.params.core_host
                ))
            else:
                warnings.warn(
                    f"Cannot interpret {type(r)} as a result."
                )

        # return results[0] if len(results) == 1 else results
        return results

        # return self.get_results(
        #     task_id=self.state.params.operation_id,
        #     # returned=returned,
        #     returned=self._returned,
        #     run_id=run_id
        # )

    def get_injectables(
        self,
    ) -> list[CoreInjectable]:
        """Retrieves a list of injections available for Malevich Core

        CoreInjectable is a representation of possible injection.
        To inject a collection into the task, you need to create
        a new collection using Malevich Core API.

        Use `get_inject_data()` as the key for `overrides` in the `run` method.

        Use `get_inject_key()` to match the alias of overriden collection.

        Returns:
            list[CoreInjectable]: A list of injections available for Malevich Core
        """
        injectables = []
        nodes_ = set()
        nodes: Iterable[BaseNode] = []
        for x in self.state.ops.values():
            nodes.append(x)
            nodes_.add(x.uuid)

        for node in nodes:
            if isinstance(node, CollectionNode):
                for cfg_coll_id, core_coll_id in self.state.cfg.collections.items():
                    if cfg_coll_id == node.collection.collection_id:
                        injectables.append(
                            CoreInjectable(
                                node=node,
                                collection_id=node.collection.collection_id,
                                alias=node.alias,
                                uploaded_id=core_coll_id
                            )
                        )

                        break

        return injectables

    def get_operations(self, *args, **kwargs) -> list[str]:
        return [
            x.alias
            for x in self.state.ops.values()
            if isinstance(x, OperationNode)
        ]

    def async_run(
        self,
        run_id: Optional[str] = None,
        override: Optional[dict[str, str]] = None,
        *args,
        **kwargs
    ) -> None:
        self.run(
            run_id=run_id,
            override=override,
            detached=True,
            *args,
            **kwargs
        )

    def async_stop(
        self,
        *args,
        **kwargs
    ) -> None:
        self.stop(*args, **kwargs)

    def async_results(
        self,
        returned: Iterable[traced[BaseNode]] | traced[BaseNode] | None,
        run_id: Optional[str] = None,
        *args,
        **kwargs
    ) -> Iterable[pd.DataFrame] | pd.DataFrame:
        return self.results(returned, run_id, *args, **kwargs)

    def async_prepare(
        self,
        stage: PrepareStages = PrepareStages.ALL,
        *args,
        **kwargs
    ) -> None:
        self.prepare(stage, *args, **kwargs)

    def commit_returned(self, returned: FlowOutput) -> None:
        self._returned = returned

    def dump(self) -> bytes:
        return pickle.dumps(self)

    def get_interpreted_task(self) -> BaseTask:
        return self

    def interpret(self, interpreter: Interpreter = None) -> None:
        raise Exception(
            "Trying to re-interpret task of type `CoreTask`. You can only interpret "
            "`PromisedTask`"
        )

    @property
    def tree(self) -> None:
        raise Exception(
            "Trying to access a tree of type `CoreTask`. Get tree from `PromisedTask`"
            " instead."
        )

    def publish(
        self,
        capture_results: list[str] | Literal['all'] | Literal['last'] = 'last',
        enable_not_auth: bool = True,
        hash: str | None = None,
        *args,
        **kwargs
    ) -> MetaEndpoint:
        from malevich_coretools import create_endpoint, update_endpoint
        if self.get_stage() != CoreTaskStage.BUILT:
            self.prepare(stage=PrepareStages.BUILD)

        cfg = deepcopy(self.state.cfg)
        if capture_results == 'last':
            cfg.app_settings = [
                x for x in cfg.app_settings
                if x.taskId == self.state.params.task_id
            ]
        elif isinstance(capture_results, list):
            task_ids = [
                self.state.task_aliases[x]
                for x in capture_results
            ]
            cfg.app_settings = [
                x for x in cfg.app_settings
                if x.taskId in task_ids
            ]
        cfg_id = f'endpoint-config-{uuid.uuid4().hex}'

        self._create_cfg_safe(
            cfg_id=cfg_id,
            cfg=cfg
        )

        injectables = self.get_injectables()
        schemes = []
        for inj in injectables:
            try:
                core.create_scheme(
                    inj.node.scheme,
                    name=f'meta_scheme_{inj.node.collection.magic()}',
                    auth=self.state.params.core_auth,
                    conn_url=self.state.params.core_host,
                )
            except Exception:
                pass
            schemes.append(f'meta_scheme_{inj.node.collection.magic()}')

        if 'prepare' not in kwargs:
            kwargs['prepare'] = True

        if not hash:
            _hash = create_endpoint(
                task_id=self.state.params.task_id,
                cfg_id=cfg_id,
                auth=self.state.params.core_auth,
                conn_url=self.state.params.core_host,
                enable_not_auth=enable_not_auth,
                expected_colls_with_schemes={
                    k.get_inject_data(): s
                    for k, s in zip(injectables, schemes)
                },
                description=(
                    self.component.description if self.component
                    else 'Auto-generated meta endpoint'
                ),
                *args,
                **kwargs
            )
        else:
            _hash = update_endpoint(
                hash,
                task_id=self.state.params.task_id,
                cfg_id=cfg_id,
                auth=self.state.params.core_auth,
                conn_url=self.state.params.core_host,
                enable_not_auth=enable_not_auth,
                expected_colls_with_schemes={
                    k.get_inject_data(): s
                    for k, s in zip(injectables, schemes)
                },
                description=(
                    self.component.description if self.component
                    else 'Auto-generated meta endpoint'
                ),
                *args,
                **kwargs
            )

        _endpoint = core.get_endpoint(
            hash=_hash,
            auth=self.state.params.core_auth,
            conn_url=self.state.params.core_host,
        )

        return MetaEndpoint(
            **_endpoint.model_dump(), conn_url=self.state.params.core_host
        )
