import enum
import hashlib
import importlib
import json
import os
import pickle
import uuid
import warnings
from copy import deepcopy
from typing import Any, Iterable, Literal, Optional, Self, Type

import malevich_coretools as core
import pandas as pd
from malevich_space.schema import ComponentSchema
from pydantic import BaseModel, ValidationError

from malevich._autoflow.tracer import traced, tracedLike
from malevich._core.ops import (
    batch_upload_collections,
)
from ...._meta.decor import ProcessorFunction
from malevich._utility import IgnoreCoreLogs, LogLevel, cout, upload_zip_asset
from ...._utility.package import PackageManager
from malevich.models import (
    Action,
    AssetNode,
    BaseNode,
    Collection,
    CollectionNode,
    CoreInjectable,
    CoreInterpreterState,
    CoreLocalDFResult,
    CoreResult,
    MetaEndpoint,
    OperationNode,
    TreeNode,
    VerbosityLevel,
)
from ...exceptions import NoPipelineFoundError, NoTaskToConnectError
from malevich.table import table
from malevich.types import FlowOutput

from ...nodes.document import DocumentNode
from ...overrides import AssetOverride, CollectionOverride, DocumentOverride, Override
from ..base import BaseTask


class BootError(Exception):
    ...


class PrepareStages(enum.Enum):
    BUILD = 0b01
    BOOT = 0b10
    ALL = 0b11


class CoreTaskStage(enum.Enum):
    ONLINE = 0
    BUILT = 1
    NO_TASK = 3


class CoreTaskState(CoreInterpreterState):
    unique_task_hash: str | None = None
    config: core.Cfg | None = None
    config_id: str | None = None
    pipeline_id: str | None = None


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
        component: ComponentSchema | None = None
    ) -> None:

        if state is None:
            raise Exception("CoreTask requires a self.state to be passed. ")

        self.state = CoreTaskState()
        for key in CoreInterpreterState.model_fields.keys():
            setattr(self.state, key, getattr(state, key))

        self.run_id = None
        self.component = component

        self._returned = None

    def _create_cfg_safe(
        self,
        auth: core.AUTH = None,
        conn_url: Optional[str] = None,
        **kwargs,
    ) -> None:
        auth = auth or self.state.params.core_auth
        conn_url = conn_url or self.state.params.core_host
        with IgnoreCoreLogs():
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

    def get_active_tasks(self) -> list[str]:
        return core.get_run_active_runs(
            auth=self.state.params.core_auth,
            conn_url=self.state.params.core_host,
        ).ids

    def get_stage(self) -> CoreTaskStage:
        if self.state.pipeline_id is not None:
            try:
                runs = core.get_run_active_runs(
                    auth=self.state.params.core_auth,
                    conn_url=self.state.params.core_host,
                ).ids
                if self.state.pipeline_id in runs:
                    return CoreTaskStage.ONLINE
            except Exception:
                pass

        if self.state.unique_task_hash is not None:
            try:
                core.get_pipeline(
                    self.state.unique_task_hash,
                    self.state.params.task_id,
                    auth=self.state.params.core_auth,
                    conn_url=self.state.params.core_host
                )
                return CoreTaskStage.BUILT
            except Exception:
                pass

        return CoreTaskStage.NO_TASK

    def get_stage_class(self) -> Type[CoreTaskStage]:
        return CoreTaskStage

    def _configure(
        self,
        operation: str,
        # Configurable parameters
        platform: str = 'base',
        platform_settings: Optional[dict[str, Any]] = None,
        # Rest of the parameters for compatibility
        **kwargs
    ) -> None:
        """internal"""
        assert platform in [
            'base', 'vast'], f"Platform {platform} is not supported. "
        cout(
            message=f"Configure {operation}: platform={platform}, platform_settings={platform_settings}",  # noqa: E501
            action=Action.Interpretation,
            verbosity=VerbosityLevel.OnlyStatus,
        )

        self.state.processors[operation].platform = platform
        if platform_settings:
            self.state.processors[operation].platformSettings = platform_settings

    def configure(
        self,
        *operations: str,
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
        for o in operations:
            self._configure(
                o,
                platform=platform,
                platform_settings=platform_settings,
                **kwargs
            )

    def get_pipeline_hash(self) -> str:
        return hashlib.sha256(
            core.Pipeline(
                pipelineId='',
                processors=self.state.processors,
                conditions={},  # NOTE: Future
                results=self.state.results
            ).model_dump_json().encode()
        ).hexdigest()

    def prepare(
        self,
        stage: PrepareStages = PrepareStages.ALL,
        *args,
        **kwargs
    ) -> tuple[str, str]:
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

        for node in self.state.collection_nodes.values():
            collection = node.collection
            try:
                collection.core_id = (
                    # No data request
                    core.get_collections_by_name(collection.magic()).ownIds[-1]
                )
            except Exception:
                collection.core_id = core.create_collection_from_df(
                    collection.collection_data,
                    name=collection.magic(),
                    conn_url=self.state.params.core_host,
                    auth=self.state.params.core_auth,
                )

        for node in self.state.asset_nodes.values():
            if node.core_path is not None:
                try:
                    files = core.get_collection_objects(
                        node.core_path,
                        recursive=True
                    ).files

                    if node.real_path is not None:
                        for file in node.real_path:
                            if file not in files:
                                raise FileNotFoundError(f'{file} missing')
                            if os.path.getsize(file) != files[file]:
                                raise FileNotFoundError(
                                    f'{file} size mismatch')

                except Exception as e:
                    if isinstance(e, FileNotFoundError):
                        message = e.strerror
                    else:
                        message = 'Failed to fetch asset'

                    url = core.post_collection_object_presigned_url(
                        node.core_path,
                        expires_in=600,
                        conn_url=self.state.params.core_host,
                        auth=self.state.params.core_auth,
                    )

                    upload_zip_asset(
                        url,
                        files=node.real_path if isinstance(node.real_path, list) else None,  # noqa: E501
                        file=node.real_path if isinstance(node.real_path, str) else None,  # noqa: E501
                    )

                    cout(
                        action=Action.Preparation,
                        message=f"Asset {node.name} updated. {message}",
                        verbosity=VerbosityLevel.AllSteps,
                        level=LogLevel.Debug
                    )

                else:
                    cout(
                        action=Action.Preparation,
                        message=f"Asset {node.name} fully matched with the Core",
                        verbosity=VerbosityLevel.AllSteps,
                        level=LogLevel.Debug
                    )

        for node in self.state.document_nodes.values():
            try:
                node.core_id = core.get_doc_by_name(
                    node.magic(),
                    conn_url=self.state.params.core_host,
                    auth=self.state.params.core_auth,
                ).id

                cout(
                    action=Action.Preparation,
                    message=f"Document {node.reverse_id} is already on Core. {node.magic()}",  # noqa: E501
                    verbosity=VerbosityLevel.AllSteps,
                    level=LogLevel.Debug
                )
            except Exception:
                node.core_id = core.create_doc(
                    data=node.document.model_dump_json(),
                    name=node.magic(),
                    conn_url=self.state.params.core_host,
                    auth=self.state.params.core_auth,
                )

                cout(
                    action=Action.Preparation,
                    message=f"Document {node.reverse_id} uploaded. {node.magic()}",
                    verbosity=VerbosityLevel.AllSteps,
                    level=LogLevel.Debug
                )

        if not self.state.config:
            config = core.Cfg(
                collections={
                    v.collection.collection_id: v.collection.core_id
                    for k, v in self.state.collection_nodes.items()
                }
            )

            for an in self.state.asset_nodes.values():
                config.collections = {
                    **config.collections,
                    an.name: an.get_core_path(),
                }

            for dn in self.state.document_nodes.values():
                config.collections = {
                    **config.collections,
                    dn.reverse_id: dn.get_core_path(),
                }

            self.state.config = config

        if stage.value & PrepareStages.BUILD.value:
            if not self.state.unique_task_hash:
                self.state.unique_task_hash = self.get_pipeline_hash()

            self.state.config_id = self.state.unique_task_hash
            try:
                with IgnoreCoreLogs():
                    self.state.pipeline_id = core.get_pipeline(
                        self.state.unique_task_hash,
                        conn_url=self.state.params.core_host,
                        auth=self.state.params.core_auth,
                    ).pipelineId

                    cout(
                        action=Action.Preparation,
                        message=f"Pipeline {self.state.unique_task_hash} found.",
                        verbosity=VerbosityLevel.AllSteps,
                        level=LogLevel.Debug
                    )

            except Exception:
                self.state.pipeline_id = core.create_pipeline(
                    self.state.unique_task_hash,
                    processors=self.state.processors,
                    conditions=None,  # NOTE: Future
                    results=self.state.results,
                    conn_url=self.state.params.core_host,
                    auth=self.state.params.core_auth,
                )

                cout(
                    action=Action.Preparation,
                    message=f"Pipeline {self.state.unique_task_hash} created.",
                    verbosity=VerbosityLevel.AllSteps,
                    level=LogLevel.Debug
                )

        if stage.value & PrepareStages.BOOT.value:
            if self.state.pipeline_id is None:
                raise BootError(
                    "Failed to boot: no pipeline found. "
                    "Try `.prepare(stage=PrepareStages.BUILD)` or reinterpret the task"
                )
            try:
                self._create_cfg_safe(
                    cfg_id=self.state.unique_task_hash,
                    cfg=self.state.config,
                    auth=self.state.params.core_auth,
                    conn_url=self.state.params.core_host
                )

                self.state.params.operation_id = core.pipeline_prepare(
                    pipeline_id=self.state.unique_task_hash,
                    cfg_id=self.state.unique_task_hash,
                    auth=self.state.params.core_auth,
                    conn_url=self.state.params.core_host,
                    *args,
                    **kwargs
                ).operationId
            except (Exception, KeyboardInterrupt) as e:
                try:
                    core.task_stop(
                        self.state.params.operation_id,
                        auth=self.state.params.core_auth,
                        conn_url=self.state.params.core_host,
                    )
                except Exception:
                    pass
                raise e

        return self.state.unique_task_hash, self.state.params.operation_id

    def _prepare_collection_overrides(
        self,
        injectables: list[CoreInjectable],
        overrides: dict[str, CollectionOverride]
    ) -> dict[str, str]:
        collections = [
            Collection(
                collection_id=f'core_interpreter_override_{k}_{self.run_id}',
                collection_data=v.data,
                persistent=False
            ) for k, v in overrides.items()
        ]

        core_ids = batch_upload_collections(
            collections,
            conn_url=self.state.params.core_host,
            auth=self.state.params.core_auth,
        )

        key_to_core_id = {
            k: core_id
            for k, core_id in zip(overrides.keys(), core_ids)
        }

        return {
            injectable.get_inject_data():
            key_to_core_id[injectable.get_inject_key()]
            for injectable in injectables
            if injectable.get_inject_key() in overrides
        }

    def _prepare_asset_overrides(
        self,
        injectables: list[CoreInjectable],
        overrides: dict[str, AssetOverride]
    ) -> dict[str, str]:
        real_overrides = {}

        for k, v in overrides.items():
            if v.path is not None:
                if v.file is not None:
                    core.update_collection_object(
                        v.path,
                        open(v.file, 'rb').read(),
                        conn_url=self.state.params.core_host,
                        auth=self.state.params.core_auth,
                    )
                elif v.files:
                    url = core.post_collection_object_presigned_url(
                        v.path,
                        conn_url=self.state.params.core_host,
                        auth=self.state.params.core_auth,
                    )
                    upload_zip_asset(
                        url,
                        files=v.files
                    )
                else:
                    real_overrides[k] = v.path

        return {
            injectable.get_inject_data():
            real_overrides[injectable.get_inject_key()]
            for injectable in injectables
            if injectable.get_inject_key() in real_overrides
        }

    def _prepare_document_overrides(
        self,
        injectables: list[CoreInjectable],
        overrides: dict[str, DocumentOverride]
    ) -> dict[str, str]:
        real_overrides = {}

        for k, v in overrides.items():
            name = hashlib.sha256(v.data.model_dump_json().encode()).hexdigest() + '_override'  # noqa: E501
            id = core.create_doc(
                data=v.data.model_dump_json(),
                name=name,
                conn_url=self.state.params.core_host,
                auth=self.state.params.core_auth,
            )
            real_overrides[k] = f'#{id}'

        return {
            injectable.get_inject_data():
            real_overrides[injectable.get_inject_key()]
            for injectable in injectables
            if injectable.get_inject_key() in real_overrides
        }

    def _validate_proc_cfg_ext(
        self,
        package_id: str,
        processor_id: str,
        config_extension,
    ):
        """internal"""
        try:
            PackageManager().get_package(package_id)
            module = importlib.import_module(f'malevich.{package_id}')
        except Exception:
            return None
        if not hasattr(module, processor_id):
            return None
        proc_stub = getattr(module, processor_id)
        if not isinstance(proc_stub, ProcessorFunction):
            return None

        if isinstance(proc_stub.config, BaseModel) and isinstance(config_extension, BaseModel):  # noqa: E501
            return type(proc_stub.config) == type(config_extension)  # noqa: E721
        else:
            return True

    def _get_config_model(self, package_id: str, processor_id: str) -> BaseModel | None:
        try:
            module = importlib.import_module(f'malevich.{package_id}')
        except ImportError:
            return None

        proc_stub = getattr(module, processor_id)
        if isinstance(proc_stub, ProcessorFunction):
            if issubclass(proc_stub.config, BaseModel):
                return proc_stub.config
        return None

    def run(
        self,
        override: dict[str, Override] | None = None,
        config_extension: dict[str, dict[str, Any] | BaseModel] | None = None,
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
            collection_overrides = {
                k: v for k, v in override.items()
                if isinstance(v, CollectionOverride)
            }
            asset_overrides = {
                k: v for k, v in override.items()
                if isinstance(v, AssetOverride)
            }

            document_overrides = {
                k: v for k, v in override.items()
                if isinstance(v, DocumentOverride)
            }

            injectables = self.get_injectables()
            real_overrides = {
                **self._prepare_collection_overrides(
                    injectables,
                    collection_overrides
                ),
                **self._prepare_asset_overrides(
                    injectables,
                    asset_overrides
                ),
                **self._prepare_document_overrides(
                    injectables,
                    document_overrides
                )
            }

            for k, v in real_overrides.items():
                cout(
                    message=f"Override {k} with {v}",
                    action=Action.Run,
                    verbosity=VerbosityLevel.AllSteps,
                    level=LogLevel.Debug
                )

        else:
            real_overrides = {}

        self.run_id = run_id or uuid.uuid4().hex

        app_cfg_extensions = {}
        if config_extension:
            for alias, extension in config_extension.items():
                for x in self.state.operation_nodes.values():
                    if isinstance(x, OperationNode) and x.alias == alias:
                        config_model: BaseModel | None = self._get_config_model(
                            x.package_id,
                            x.processor_id
                        )
                        if not isinstance(extension, BaseModel) and not isinstance(extension, dict):  # noqa: E501
                            _expected = "dictionary"
                            if config_model is not None and isinstance(config_model, BaseModel):  # noqa: E501
                                _expected += f'or {type(config_model).__name__}'

                            raise ValueError(
                                "Invalid type for config extension. "
                                f"Expected {_expected}, but found {type(extension).__name__} "  # noqa: E501
                                f"for alias {x.alias}, processor {x.processor_id} and "
                                f"package {x.package_id}."
                            )

                        if config_model is not None:
                            try:
                                config_model(**{
                                    **x.config,
                                    **(extension if isinstance(extension, dict)
                                       else extension.model_dump()
                                       )
                                })
                            except ValidationError as e:
                                raise ValueError(
                                    "Failed to extend the configuration "
                                    f"for processor {x.processor_id} with alias"
                                    f" {x.alias}. New configuration do not comprise "
                                    "to the schema of the processor of the config. "
                                    "See validation errors above."
                                ) from e
                            if (
                                config_model is not None
                                and issubclass(config_model, BaseModel)
                                and not issubclass(config_model,  type(extension))
                            ):
                                raise ValueError(
                                    "Failed to extend the configuration "
                                    f"for processor {x.processor_id} with alias"
                                    f" {x.alias}. Expected {config_model.__name__}, "
                                    f"but the configuration extenstion is {type(extension).__name__}"  # noqa: E501
                                )

                        if isinstance(extension, BaseModel):
                            extension_json = extension.model_dump_json()
                        elif isinstance(extension, dict):
                            extension_json = json.dumps(extension)
                        else:
                            # ok, validates before
                            pass

                        app_cfg_extensions['$' + x.alias] = extension_json

        try:
            if real_overrides or app_cfg_extensions:
                new_config = self.state.config.model_copy(deep=True)
                new_config.collections = {
                    **self.state.config.collections,
                    **real_overrides,
                }
                new_config.app_cfg_extension = app_cfg_extensions
                new_config_id = self.state.config_id + \
                    '_' + uuid.uuid4().hex[:6]
                self._create_cfg_safe(
                    cfg_id=new_config_id,
                    cfg=new_config,
                    conn_url=self.state.params.core_host,
                    auth=self.state.params.core_auth,
                )
                core.task_run(
                    self.state.params.operation_id,
                    cfg_id=new_config_id,
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
            # core.task_stop(
            #     self.state.params.operation_id,
            #     auth=self.state.params.core_auth,
            #     conn_url=self.state.params.core_host,
            # )
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
    ) -> list[CoreResult | CoreLocalDFResult]:
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
                    core_group_name=r.owner.alias,
                    core_operation_id=self.state.params.operation_id,
                    core_run_id=run_id,
                    auth=self.state.params.core_auth,
                    conn_url=self.state.params.core_host,
                ))
            elif isinstance(node, AssetNode):
                results.append(CoreResult(
                    core_group_name=r.owner.alias,
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
        injectables.extend([
            CoreInjectable(
                alias=node.alias,
                node=node,
                collection_id=node.collection.collection_id,
            ) for node in self.state.collection_nodes.values()])

        injectables.extend([
            CoreInjectable(
                alias=node.alias,
                node=node,
                collection_id='$' + (node.core_path or node.real_path)
            ) for node in self.state.asset_nodes.values()])

        injectables.extend([
            CoreInjectable(
                alias=node.alias,
                node=node,
                collection_id='#' + (node.core_id or node.reverse_id)
            ) for node in self.state.document_nodes.values()])

        return injectables

    def get_operations(self, *args, **kwargs) -> list[str]:
        return [
            x.alias
            for x in self.state.operation_nodes.values()
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

    def interpret(self, interpreter: 'Interpreter' = None) -> None:
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

    def connect(
        self,
        unique_task_hash: str | None = None,
        only_fetch: bool = False,
    ) -> Self:
        unique_task_hash = unique_task_hash or self.get_pipeline_hash()
        try:
            pipeline = core.get_pipeline(
                id=unique_task_hash,
                conn_url=self.state.params.core_host,
                auth=self.state.params.core_auth
            )

        except Exception:
            raise NoPipelineFoundError(unique_task_hash)

        self.state.processors = pipeline.processors
        self.state.results = pipeline.results
        self.state.conditions = pipeline.conditions
        self.state.unique_task_hash = unique_task_hash
        self.state.config = core.Cfg()

        json_cfg = json.loads(core.get_cfg(
            unique_task_hash,
            conn_url=self.state.params.core_host,
            auth=self.state.params.core_auth
        ).data)

        for key, value in json_cfg.items():
            setattr(self.state.config, key, value)

        if self.state.operation_nodes:
            node_results = [
                tracedLike(self.state.operation_nodes[x])
                for x in pipeline.results.keys()
            ]
        else:
            node_results = [
                tracedLike(OperationNode(alias=x, operation_id=''))
                for x in pipeline.results.keys()
            ]

        self.commit_returned(node_results)
        if not only_fetch:
            tasks = self.get_active_tasks()

            if tasks:
                self.state.params.operation_id = tasks[-1]
                cout(
                    Action.Attachment,
                    message='Connected to online task. ' + tasks[-1],
                    level=LogLevel.Info,
                    verbosity=VerbosityLevel.OnlyStatus
                )
            else:
                raise NoTaskToConnectError()

        return self

    def publish(
        self,
        capture_results: list[str] | Literal['all'] | Literal['last'] = 'last',
        enable_not_auth: bool = True,
        hash: str | None = None,
        *args,
        **kwargs
    ) -> MetaEndpoint:

        from malevich_coretools import create_endpoint, update_endpoint
        if self.get_stage() not in [CoreTaskStage.BUILT, CoreTaskStage.ONLINE]:
            self.prepare(stage=PrepareStages.BUILD)

        cfg = deepcopy(self.state.config)
        cfg_id = f'endpoint-config-{uuid.uuid4().hex}'

        self._create_cfg_safe(
            cfg_id=cfg_id,
            cfg=cfg
        )

        if 'prepare' not in kwargs:
            kwargs['prepare'] = True

        if not hash:
            _hash = create_endpoint(
                task_id=self.state.unique_task_hash,
                cfg_id=cfg_id,
                auth=self.state.params.core_auth,
                conn_url=self.state.params.core_host,
                enable_not_auth=enable_not_auth,
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
                task_id=self.state.unique_task_hash,
                cfg_id=cfg_id,
                auth=self.state.params.core_auth,
                conn_url=self.state.params.core_host,
                enable_not_auth=enable_not_auth,
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
            **_endpoint.model_dump(),
            conn_url=self.state.params.core_host
        )
