from typing import Any, Optional
import uuid

from malevich.models import CoreInjectable
import malevich_coretools as core
from malevich_app import LocalRunner, LocalRunStruct, cfg_translate, pipeline_translate
from pydantic import BaseModel

from malevich._utility.logging import cout
from malevich.models.actions import Action
from malevich.models.nodes.asset import AssetNode
from malevich.models.nodes.base import BaseNode
from malevich.models.nodes.collection import CollectionNode
from malevich.models.nodes.morph import MorphNode
from malevich.models.nodes.tree import TreeNode
from malevich.models.overrides import DocumentOverride, Override
from malevich.models.results.local.infs import LocalResult
from malevich.models.state.local import LocalInterpreterState
from malevich.models.task.interpreted.core import CoreTask
from malevich.models.overrides import DocumentOverride, CollectionOverride, AssetOverride
from malevich.models.collection import Collection
from malevich.path import Paths


class LocalTaskState(LocalInterpreterState):
    config: core.Cfg | None = None
    config_id: str | None = None
    pipeline_id: str | None = None
    unique_task_hash: str | None = None
    operation_id: str | None = None

class LocalTask(CoreTask):
    def __init__(self, state: LocalTaskState) -> None:
        self._returned = None
        self.state = LocalTaskState()

        for key in LocalInterpreterState.model_fields.keys():
            setattr(self.state, key, getattr(state, key))

        self.run_id = None
        self.component = None
        self.__struct = LocalRunStruct(
            import_dirs=list(self.state.import_paths),
            results_dir=Paths.local('results', create_dir=True),
            mount_path=Paths.local('mnt', create_dir=True),
            mount_path_obj=Paths.local('mnt_obj', create_dir=True),
        )
        self.runner = LocalRunner(
            local_settings=self.__struct,
        )

    def get_active_tasks(self) -> None:
        raise RuntimeError(
            "Cannot get active tasks in local mode"
        )

    def get_stage(self) -> None:
        raise RuntimeError(
            "Cannot get stage in local mode"
        )

    def configure(*args, **kwargs) -> None:
        raise RuntimeError(
            "Cannot configure in local mode"
        )

    async def prepare(
        self,
        *args,
        **kwargs
    ) -> tuple[str, str]:
        for node in self.state.asset_nodes.values():
            if node.real_path is None or node.core_path is None:
                raise ValueError(
                    "Real and remote paths must be specified for all assets"
                    " when using local mode."
                )

            self.runner.storage.obj(
                obj_path=node.core_path,
                path_from=node.real_path,
            )

        for node in self.state.document_nodes.values():
            node.core_id = self.runner.storage.data(
                data=node.document.model_dump() if isinstance(node.document, BaseModel) else node.document,
                id=node.core_id,
            )

        for node in self.state.collection_nodes.values():
            node.collection.core_id = self.runner.storage.data(
                data=node.collection.collection_data,
                id=node.collection.core_id,
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

        self.state.operation_id = await self.runner.prepare(
            pipeline_translate(self.get_pipeline(with_hash=False)),
            cfg_translate(self.state.config),
            **kwargs
        )
        
    def _prepare_collection_overrides(
        self,
        injectables: list[CoreInjectable],
        overrides: dict[str, CollectionOverride]
    ) -> dict[str, str]:

        return {
            key: self.runner.storage.data(ref.data)
            for key, ref in overrides.items()
        }


    def _prepare_document_overrides(
        self,
        injectables: list[CoreInjectable],
        overrides: dict[str, DocumentOverride]
    ) -> dict[str, str]:
        return {
            key: '#' + self.runner.storage.data(ref.data.model_dump() if isinstance(ref.data, BaseModel) else ref.data)
            for key, ref in overrides.items()
        }

    async def run(
        self,
        override: dict[str, Override] | None = None,
        config_extension: dict[str, dict[str, Any] | BaseModel] | None = None,
        run_id: Optional[str] = None,
        *args,
        **kwargs
    ) -> None:
        self.run_id = run_id or uuid.uuid4().hex

        if override:
            real_overrides = self._compile_overrides(override)
        else:
            real_overrides = {}

        if config_extension:
            print(config_extension, self._validate_extension(config_extension))
            app_cfg_extensions = self._validate_extension(config_extension)
        else:
            app_cfg_extensions = {}

        if real_overrides or app_cfg_extensions:
            new_config = self.state.config.model_copy(deep=True)
            new_config.collections = {
                **self.state.config.collections,
                **real_overrides,
            }

            new_config.app_cfg_extension = app_cfg_extensions
            print(new_config.app_cfg_extension)
            await self.runner.run(
                *args,
                run_id=self.run_id,
                operation_id=self.state.operation_id,
                cfg=cfg_translate(new_config),
                **kwargs
            )
        else:
            await self.runner.run(
                *args,
                run_id=self.run_id,
                operation_id=self.state.operation_id,
                cfg=cfg_translate(self.state.config),
                **kwargs
            )

        return self.run_id

    async def stop(self, *args, **kwargs) -> None:
        await self.runner.stop(
            *args,
            run_id=self.run_id,
            operation_id=self.state.operation_id,
            **kwargs
        )

    async def results(
        self,
        run_id: Optional[str] = None,
        *args,
        **kwargs
    ) -> list[LocalResult]:
        cout(
            message=f"Scanning results. {Paths.local('results', self.state.operation_id, self.run_id)}",  # noqa: E501
            action=Action.Results
        )
        if self.state.operation_id is None:
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

        no_conditions_return = None
        final_result = None

        condition_map = {}
        for condition, return_map in returned:
            if condition is None:
                no_conditions_return = return_map

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
        results = [
            LocalResult(
                operation_id=self.state.operation_id,
                run_id=self.run_id,
                name=node.owner.alias,
                login=self.__struct.login,
            )
            for node in returned
        ]

        return results
