import enum
import hashlib
import importlib
import json
import os
import pickle
import uuid
import warnings
from copy import deepcopy
from typing import Any, Iterable, Literal, Optional, Type

import malevich_coretools as core
import pandas as pd
from malevich_space.schema import ComponentSchema
from pydantic import BaseModel, ValidationError

from malevich._autoflow.tracer import traced, tracedLike
from malevich._core.ops import (
    batch_upload_collections,
)
from malevich._utility import IgnoreCoreLogs, LogLevel, cout, upload_zip_asset
from ...nodes.morph import MorphNode
from ...._utility.cache.manager import CacheManager
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
from malevich.types import FlowOutput

from ...._utility.package import PackageManager
from ...exceptions import NoPipelineFoundError, NoTaskToConnectError
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

    supports_conditional_output = True

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

        CacheManager().core.write_entry(
            self.get_pipeline().model_dump_json(indent=4),
            entry_name=self.get_pipeline_hash() + '.json',
            entry_group='pipelines',
            force_overwrite=True
        )

    def get_active_tasks(self) -> list[str]:
        return self.state.service.run.active.list().ids

    def get_stage(self) -> CoreTaskStage:
        if self.state.pipeline_id is not None:
            try:
                with IgnoreCoreLogs():
                    runs = self.get_active_tasks()
                    if self.state.pipeline_id in runs:
                        return CoreTaskStage.ONLINE
            except Exception:
                pass

        if self.state.unique_task_hash is not None:
            try:
                with IgnoreCoreLogs():
                    self.state.service.pipeline.id(
                        self.state.unique_task_hash
                    ).get()
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

    def get_pipeline(self, with_hash=False) -> core.Pipeline:
        pipeline = core.Pipeline(
            pipelineId='',
            processors=self.state.processors,
            conditions=self.state.conditions,
            results=self.state.results
        )
        if with_hash:
            pipeline.pipelineId = self.get_pipeline_hash()

        return pipeline

    def get_pipeline_hash(self) -> str:
        return hashlib.sha256(
           self.get_pipeline(with_hash=False).model_dump_json().encode() 
        ).hexdigest()

    async def prepare(
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
        service = self.state.service
        for node in self.state.collection_nodes.values():
            collection = node.collection
            collection.core_id = (
                service.collection.name(collection.magic())
                .update_or_create(collection.collection_data)
            )

        for node in self.state.asset_nodes.values():
            if node.core_path is not None:
                with IgnoreCoreLogs():
                    try:
                        try:
                            files = service.asset.path(node.core_path).list(
                                recursive=True
                            ).files
                        except Exception as fe:
                            try:
                                files = service.asset.path(node.core_path).get()
                            except Exception as e:
                                raise fe from e



                        if node.real_path is not None:
                            if isinstance(files, bytes):
                                if isinstance(node.real_path, str):
                                    if os.path.getsize(node.real_path) != len(files):
                                        raise FileNotFoundError(
                                            f'{node.real_path} size mismatch'
                                        )
                                elif isinstance(node.real_path, list) and len(node.real_path) == 1:  # noqa: E501
                                    if os.path.getsize(node.real_path[0]) != len(files):
                                        raise FileNotFoundError(
                                            f'{node.real_path} size mismatch'
                                        )
                                else:
                                    raise FileNotFoundError(
                                        "Multiple files specified, but core asset is a single file"  # noqa: E501
                                    )
                            else:
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

                        service.asset.path(node.core_path).create(
                            file=node.real_path if isinstance(node.real_path, str) else None,  # noqa: E501
                            files=node.real_path if isinstance(node.real_path, list) else None,  # noqa: E501
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
                ref = service.document.name(node.magic())
                with IgnoreCoreLogs():
                    node.core_id = ref.get().id
                cout(
                    action=Action.Preparation,
                    message=f"Document {node.reverse_id} is already on Core. {node.magic()}",  # noqa: E501
                    verbosity=VerbosityLevel.AllSteps,
                    level=LogLevel.Debug
                )
            except Exception:
                node.core_id = ref.create(data=node.dump_document_json())

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
                pref = service.pipeline.id(self.state.unique_task_hash)
                with IgnoreCoreLogs():
                    self.state.pipeline_id = pref.get().pipelineId
                    cout(
                        action=Action.Preparation,
                        message=f"Pipeline {self.state.unique_task_hash} found.",
                        verbosity=VerbosityLevel.AllSteps,
                        level=LogLevel.Debug
                    )

            except Exception:
                self.state.pipeline_id = pref.create(
                    processors=self.state.processors,
                    conditions=self.state.conditions or None,
                    results=self.state.results,
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
                service.cfg.name(self.state.config_id).update_or_create(
                    cfg_id=self.state.config_id,
                    cfg=self.state.config,
                )
                piperef = service.pipeline.id(self.state.unique_task_hash)
                self.state.params.operation_id = piperef.prepare(
                    cfg_id=self.state.unique_task_hash,
                    *args,
                    **kwargs
                ).operationId
            except (Exception, KeyboardInterrupt) as e:
                try:
                    service.run.operation_id(self.state.params.operation_id).stop()
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

        refs = [
            self.state.service.collection.name(collection.magic())
            for collection in collections
        ]

        core_ids = [
            ref.update_or_create(data=col.collection_data)
            for ref, col in zip(refs, collections)
        ]

        key_to_core_id = {
            k: core_id
            for k, core_id in zip(overrides.keys(), core_ids)
        }

        return {
            injectable.get_inject_key():
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
                if v.file is not None or v.files is not None:
                    self.state.service.asset.path(v.path).create(
                        file=v.file,
                        files=v.files
                    )
                else:
                    real_overrides[k] = v.path

        return {
            injectable.get_inject_key():
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
            id = self.state.service.document.name(name).update_or_create(
                data=v.data.model_dump_json(),
            )
            real_overrides[k] = f'#{id}'

        return {
            injectable.get_inject_key():
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
        from malevich._meta.decor import ProcessorFunction
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
        from malevich._meta.decor import ProcessorFunction

        try:
            module = importlib.import_module(f'malevich.{package_id}')
        except ImportError:
            return None

        proc_stub = getattr(module, processor_id)
        if isinstance(proc_stub, ProcessorFunction):
            if issubclass(proc_stub.config, BaseModel):
                return proc_stub.config
        return None

    def _validate_extension(
        self,
        config_extension: dict[str, dict[str, Any] | BaseModel],
    ) -> str:
        app_cfg_extensions = {}
        print([*self.state.operation_nodes.values()])
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

        return app_cfg_extensions

    def _compile_overrides(
        self,
        override: dict[str, Override]
    ):
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

        return real_overrides


    async def run(
        self,
        override: dict[str, Override] | None = None,
        config_extension: dict[str, dict[str, Any] | BaseModel] | None = None,
        run_id: Optional[str] = None,
        detached: bool = False,
        stop_on_error: bool = False,
        stop_on_interrupt: bool = False,
        *args,
        **kwargs
    ) -> str:
        """Runs the task on Malevich Core

        You can supply new data or new configuration each run.

        New data is supplied using overrides. An override is a dictionary
        that maps the alias of the data node (e.g. collection, asset, document)
        to the new value via Override interface.

        Collections overrides are created using :class:`CollectionOverride`
        class.

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
            real_overrides = self._compile_overrides(override)
        else:
            real_overrides = {}

        self.run_id = run_id or uuid.uuid4().hex

        app_cfg_extensions = {}
        if config_extension:
            app_cfg_extensions = self._validate_extension(config_extension)

        tref = self.state.service.run.operation_id(
            self.state.params.operation_id
        )
        try:
            if real_overrides or app_cfg_extensions:
                new_config = self.state.config.model_copy(deep=True)
                new_config.collections = {
                    **self.state.config.collections,
                    **real_overrides,
                }
                print(self.state.config.collections, real_overrides)
                new_config.app_cfg_extension = app_cfg_extensions
                new_config_id = self.state.config_id + \
                    '_' + uuid.uuid4().hex[:6]

                self.state.service.cfg.name(new_config_id).update_or_create(
                    cfg_id=new_config_id,
                    cfg=new_config,
                )
                tref.run(
                    cfg_id=new_config_id,
                    wait=not detached,
                    run_id=self.run_id,
                    *args,
                    **kwargs
                )
            else:
                tref.run(
                    *args,
                    run_id=self.run_id,
                    wait=not detached,
                    **kwargs
                )
        except Exception as e:
            if stop_on_error:
                await self.stop()
            raise e
        except KeyboardInterrupt:
            if stop_on_interrupt:
                await self.stop()
            raise
        return self.run_id

    async def stop(
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
        self.state.service.run.operation_id(
            self.state.params.operation_id
        ).stop(*args, **kwargs)

    async def results(
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

        demorphed_returned = []
        for i in range(len(returned)):
            if isinstance(returned[i][1].owner, MorphNode):
                for morph_conditions, node in returned[i][1].owner.members:
                    demorphed_returned.append(({
                        **(returned[i][0] or {}),
                        **(morph_conditions or {})
                        }, node,
                    ))
            else:
                demorphed_returned.append(returned[i])

        returned = demorphed_returned

        if not run_id:
            run_id = self.run_id

        logs = core.logs(
            self.state.params.operation_id,
            run_id=run_id,
            auth=self.state.params.core_auth,
            conn_url=self.state.params.core_host,
        )
        no_conditions_return = None
        final_result = None

        condition_map = {}
        for alias, info in logs.pipeline.conditions.items():
            condition_map[alias] =  info[max(info.keys())]

        for condition, return_map in returned:
            if condition is None:
                no_conditions_return = return_map
            else:
                matched = True
                for node, value in condition.items():
                    matched &= condition_map[node.alias] == value
                if matched:
                    final_result = return_map

        if not condition_map:
            final_result = no_conditions_return

        returned = final_result

        if not isinstance(returned, list):
            returned = [returned]

        def _deflat(li: list[BaseNode]) -> list[BaseNode]:
            temp_returned = []
            for r in li:
                if isinstance(r, TreeNode):
                    temp_returned.extend(
                        _deflat(r.results)
                    )
                else:
                    temp_returned.append(r)
            return temp_returned

        returned = _deflat(returned)
        returned = [x.owner for x in returned]
        results = []
        for node in returned:
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
                    core_group_name=node.alias,
                    core_operation_id=self.state.params.operation_id,
                    core_run_id=run_id,
                    auth=self.state.params.core_auth,
                    conn_url=self.state.params.core_host,
                ))
            elif isinstance(node, AssetNode):
                results.append(CoreResult(
                    core_group_name=node.alias,
                    core_operation_id=self.state.params.operation_id,
                    core_run_id=run_id,
                    conn_url=self.state.params.core_host
                ))
            else:
                warnings.warn(
                    f"Cannot interpret {type(node)} as a result."
                )

        return results

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

    def commit_returned(
        self, returned: FlowOutput | dict[dict[str, bool], FlowOutput]
    ) -> None:
        self._returned = returned

    def dump(self) -> bytes:
        return pickle.dumps(self)

    def get_interpreted_task(self) -> BaseTask:
        return self

    def interpret(self, interpreter = None) -> None:
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
    ) -> 'CoreTask':
        unique_task_hash = unique_task_hash or self.get_pipeline_hash()
        try:
            pipeline = self.state.service.pipeline.id(unique_task_hash).get()
        except Exception:
            raise NoPipelineFoundError(unique_task_hash)

        self.state.processors = pipeline.processors
        self.state.results = pipeline.results
        self.state.conditions = pipeline.conditions
        self.state.unique_task_hash = unique_task_hash
        self.state.config = core.Cfg()
        self.state.config_id = unique_task_hash

        json_cfg = json.loads(
            self.state.service.cfg.id(unique_task_hash).get().data
        )

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
                pipeline_id=self.state.pipeline_id,
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
                pipeline_id=self.state.pipeline_id,
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
