import pickle
import uuid
from enum import Enum
from functools import cache
from threading import Thread
from typing import Callable, Iterable, Optional, Type

import pandas as pd
from gql import gql
from malevich_coretools.abstract.statuses import AppStatus, TaskStatus
from malevich_space.schema import LoadedComponentSchema

from malevich.core_api import BasePlatformSettings
from malevich.models.injections import SpaceInjectable

from ...._autoflow.tracer import traced
from ....types import FlowOutput
from ...nodes.base import BaseNode
from ...nodes.tree import TreeNode
from ...results.space.collection import SpaceCollectionResult
from ..base import BaseTask


class SpaceTaskStage(Enum):
    INTERPRETED     = "interpreted"
    # from Space definition
    GENERATED       = "generated"
    STARTED         = "started"
    STOPPED         = "stopped"



class SpaceTask(BaseTask):
    @staticmethod
    def load(object_bytes) -> 'SpaceTask':
        return pickle.loads(object_bytes)

    @property
    def tree(self) -> TreeNode:
        return self.state.aux.tree

    @property
    def component(self) -> LoadedComponentSchema:
        self.upload()
        return self._component

    @property
    def allow_configurations(self) -> bool:
        return self._component is None

    def __init__(
        self,
        state: 'SpaceInterpreterState',
        component: LoadedComponentSchema | None = None,
        get_component: Callable[..., LoadedComponentSchema] | None = None,
    ) -> None:
        super().__init__()
        assert component is not None or get_component is not None

        self.state = state
        self._component = component
        self.__get_component = get_component
        self._returned = []

    def _deflate(
        self,
        returned: list[traced[BaseNode]],
        alias2infid: dict[str, str]
    ) -> tuple[list[str], list[str]]:
        results_ = []
        infid_ = []
        for x in returned:
            if isinstance(x.owner, TreeNode):
                results_ = x.owner.results
                if not isinstance(results_, list):
                    results_ = [results_]
                returned_ = self._deflate(
                    results_,
                    alias2infid
                )
                results_.extend(returned_[0])
                infid_.extend(returned_[1])
            else:
                results_.append(x.owner.uuid)
                infid_.append(alias2infid[x.owner.alias])
        return results_, infid_

    def prepare(
        self,
        use_v1: bool = False,
        *args,
        **kwargs
    ) -> None:
        if not self.state.aux.flow_id:
            self.state.aux.flow_id = self.component.flow.uid

        if use_v1:
            task_id = self.state.space.build_task(
                flow_id=self.component.flow.uid,
                host_id=self.state.component_manager.host.uid
            )
            self.state.aux.task_id = task_id[0]
        else:
            task_id = self.state.space.build_task_v2(
                flow_id=self.component.flow.uid,
                host_id=self.state.component_manager.host.uid
            )
            self.state.aux.task_id = task_id['uid']
            self.state.aux.core_task_id = task_id['coreId']


        self.state.space.boot_task(
            task_id=self.state.aux.task_id,
        )

    def run(
        self,
        override: dict[str, pd.DataFrame] = {},
        webhook_url: str | None = None,
        *args,
        **kwargs
    ) -> str:
        if not self.state.aux.task_id:
            raise Exception(
                "Attempt to run a task which is not prepared. "
                "Please prepare the task first."
            )

        start_schema = self.state.space.get_task_start_schema(
            self.state.aux.task_id,
        )

        coll_override = {
            **self.state.collection_overrides
        }

        __override = {}
        for alias, df in override.items():
            for inj in self.get_injectables():
                if alias == inj.alias:
                    __override[inj.snapshot_flow_id] = df
        override = __override

        in_flow_ca = {}
        id_to_ca = {}
        overrides = []
        for sch in start_schema:
            try:
                in_flow_ca = self.state.space.get_ca_in_flow(
                    flow_id=self.state.aux.flow_id,
                    in_flow_id=sch.in_flow_id
                )
                id_to_ca[sch.in_flow_id] = in_flow_ca
            except Exception:
                # TODO fix!
                continue

        for in_flow_id, df in override.items():
            uid = self.state.space.create_collection(
                host_id=self.state.host.uid,
                # core_id=f'override-{coll_id}-{state.interpretation_id}',
                core_alias=f'override-{in_flow_id}-{self.state.interpretation_id}-{uuid.uuid4().hex[:4]}',
                # schema_core_id=schema.core_id,
                docs=[
                    row.to_json()
                    for _, row in df.iterrows()
                ]
            )
            coll_override[id_to_ca[in_flow_id]] = uid


        for sch in start_schema:
            try:
                in_flow_ca = id_to_ca[sch.in_flow_id]
                overrides.append({
                    "inFlowCompUid": sch.in_flow_id,
                    "caUid": coll_override[in_flow_ca],
                    "caAlias": sch.injected_alias
                })
            except Exception:
                # TODO fix!
                continue

        self.state.aux.run_id = self.state.space.run_task(
            task_id=self.state.aux.task_id,
            ca_override=overrides,
            webhook=webhook_url,
        )
        return self.state.aux.run_id

    def configure(
        self,
        *operations: str,
        # Configurable parameters
        platform: str = 'base',
        platform_settings: BasePlatformSettings | str = None,
        # Rest of the parameters for compatibility
        **kwargs
    ) -> None:
        if not self.allow_configurations:
            raise Exception(
                "Intepreter already uploaded a new component to Malevich Space, so "
                "configurations are not available. To configure the task, use "
                "`Space(configurable=True)` and call `upload()` afterwards to apply "
                "custom configurations"
            )
        for operation in operations:
            for component in self.state.flow.components:
                if component.alias == operation:
                    component.limits = platform_settings

    def get_interpreted_task(self) -> BaseTask:
        return self

    def get_stage(self) -> SpaceTaskStage:
        if not self.state.aux.task_id:
            return SpaceTaskStage.INTERPRETED

        request = gql(
            """
            query GetTaskBootState($task_id: String!) {
                task(uid: $task_id) {
                    details {
                    bootState
                    }
                }
            }
            """
        )
        response = self.state.space.client.execute(
            request,
            variable_values={'task_id': self.state.aux.task_id}
        )
        state = response['task']['details']['bootState']
        try:
           return SpaceTaskStage[state.upper()]
        except Exception:
            import warnings
            warnings.warn(
                "API returned bootState that "
                "is not in `SpaceTaskStage` enumerator"
            )
            return SpaceTaskStage.INTERPRETED

    def get_stage_class(self) -> Type[SpaceTaskStage]:
        return SpaceTaskStage

    def interpret(self, interpreter=None) -> None:
        return None

    def dump(self) -> bytes:
        return pickle.dumps(self)

    def stop(self, *args, **kwargs) -> None:
        self.state.space.change_task_state(
            task_id=self.state.aux.task_id,
            target_state='stop'
        )
        return

    @cache
    def get_injectables(self) -> list[SpaceInjectable]:
        if self.state.aux.task_id:
            alias_to_snapshot = self.state.space.get_snapshot_components(
                task_id=self.state.aux.task_id
            )
        else:
            alias_to_snapshot = {}

        alias_to_in_flow_id = {
            x.alias: x.uid
            for x in self.component.flow.components
            if x.collection is not None
        }

        alias_to_rev_id = {
            x.alias: x.reverse_id
            for x in self.component.flow.components
            if x.collection is not None
        }

        aliases = set()
        # aliases.update(alias_to_snapshot.keys())
        aliases.update(alias_to_in_flow_id.keys())
        return [
            SpaceInjectable(
                alias=a,
                in_flow_id=alias_to_in_flow_id.get(a, None),
                snapshot_flow_id=alias_to_snapshot.get(a, None),
                reverse_id=alias_to_rev_id.get(a, None)
            )
            for a in aliases
        ]

    def get_operations(self) -> list[str]:
        return [x.alias for x in self.get_injectables()]

    def commit_returned(self, returned: FlowOutput) -> None:
        self._returned = returned

    async def async_results(
        self,
        run_id: Optional[str] = None,
        fetch_timeout: int = 150,
        *args,
        **kwargs
    ) -> Iterable[SpaceCollectionResult]:
        """internal"""
        returned = self._returned
        if returned is None:
            return None

        if isinstance(returned, traced):
            returned = [returned]

        alias2infid = self.state.space.get_snapshot_components(
            run_id=run_id or self.state.aux.run_id
        )

        infid2alias = {
            k: v for v, k in alias2infid.items()
        }

        _, infid_ = self._deflate(
            returned,
            alias2infid
        )

        rs_, cs_ = self.state.space.get_run_status(
            run_id=self.state.aux.run_id
        )

        if rs_ == AppStatus.FAIL:
            raise Exception(
                "Run failed. No results could be fetched..."
            )

        exc_message = None
        if rs_ != AppStatus.COMPLETE.value:
            finished_ = {
                x.in_flow_comp_id for x in cs_ if x.status == AppStatus.COMPLETE.value
            }
            to_be_finished_ = set(infid_)
            async for update in self.state.space.subscribe_to_status(
                run_id or self.state.aux.run_id,
                fetch_timeout
            ):
                if isinstance(update, str):
                    if update == TaskStatus.COMPLETE.value:
                        break
                    if update == TaskStatus.FAIL.value:
                        exc_message = "Run failed. No results could be fetched..."
                        break

                else:
                    if update.status == AppStatus.FAIL.value:
                        exc_message = (
                            f"App {infid2alias[update.in_flow_comp_id]} failed. "
                            "No results could be fetched..."
                        )
                    if update.status == AppStatus.COMPLETE.value:
                        finished_.add(update.in_flow_comp_id)

                    if finished_ == to_be_finished_:
                        break

        if exc_message:
           raise Exception(exc_message)

        return [
            SpaceCollectionResult(
                run_id=run_id or self.state.aux.run_id,
                in_flow_id=i,
                space_ops=self.state.space
            ) for i in infid_
        ]

    def results(
        self,
        run_id: Optional[str] = None,
        fetch_timeout: int = 150,
        *args,
        **kwargs
    ) -> list[SpaceCollectionResult]:
        import asyncio
        import warnings
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore", message="There is no current event loop"
            )
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    result = None
                    def worker():
                        nonlocal result
                        result = asyncio.run(
                            self.async_results(run_id, fetch_timeout, *args, **kwargs)
                        )

                    thread = Thread(target=worker,)
                    thread.start()
                    thread.join()
                    assert result is not None
                    return result
            except RuntimeError:
                loop = asyncio.new_event_loop()

        def raise_exc(e, *args) -> None:
            raise e

        loop.set_exception_handler(raise_exc)
        return loop.run_until_complete(self.async_results(
            run_id,
            *args,
            **kwargs
        ))

    def upload(self, branch: str | None = None) -> None:
        if not self._component:
            self._component = self.__get_component(branch=branch)

