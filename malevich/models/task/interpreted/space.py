import pickle
from typing import Any, Iterable, Optional

import pandas as pd
from malevich_space.schema import InFlowAppSchema, LoadedComponentSchema

from malevich_coretools.abstract.statuses import AppStatus
from malevich.models.injections import BaseInjectable

from ...._autoflow.tracer import traced
from ....interpreter.space import SpaceInterpreterState
from ...nodes.base import BaseNode
from ...nodes.tree import TreeNode
from ...results.space.collection import SpaceCollectionResult
from ...types import FlowOutput
from ..base import BaseTask


class SpaceTask(BaseTask):
    @staticmethod
    def load(object_bytes) -> 'SpaceTask':
        return pickle.loads(object_bytes)

    @property
    def tree(self) -> TreeNode:
        return self.state.aux['tree']

    def __init__(
        self,
        state: SpaceInterpreterState,
        component: LoadedComponentSchema
    ) -> None:
        super().__init__()
        self.state = state
        self.component = component
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
        self.state.aux['flow_id'] = self.component.flow.uid
        if use_v1:
            task_id = self.state.space.build_task(
                flow_id=self.component.flow.uid,
                host_id=self.state.component_manager.host.uid
            )
            self.state.aux['task_id'] = task_id[0]
        else:
            task_id = self.state.space.build_task_v2(
                flow_id=self.component.flow.uid,
                host_id=self.state.component_manager.host.uid
            )
            self.state.aux['task_id'] = task_id['uid']
            self.state.aux['core_task_id'] = task_id['coreId']


        self.state.space.boot_task(
            task_id=self.state.aux['task_id'],
        )

    def run(
        self,
        override: dict[str, pd.DataFrame] = [],
        *args,
        **kwargs
    ) -> None:
        if not self.state.aux.get('task_id'):
            raise Exception(
                "Attempt to run a task which is not prepared. "
                "Please prepare the task first."
            )

        start_schema = self.state.space.get_task_start_schema(
            self.state.aux['task_id'],
        )

        in_flow_ca = {}
        overrides = []
        for sch in start_schema:
            try:
                in_flow_ca = self.state.space.get_ca_in_flow(
                    flow_id=self.state.aux['flow_id'],
                    in_flow_id=sch.in_flow_id
                )

                overrides.append({
                    "inFlowCompUid": sch.in_flow_id,
                    "caUid": self.state.collection_overrides[in_flow_ca],
                    "caAlias": sch.injected_alias
                })
            except Exception:
                # TODO fix!
                continue

        self.state.aux['run_id'] = self.state.space.run_task(
            task_id=self.state.aux['task_id'],
            ca_override=overrides
        )

    def configure(self, operation: str, **kwargs) -> None:
        return None

    def get_interpreted_task(self) -> BaseTask:
        return None

    def get_stage(self) -> Any:  # noqa: ANN401
        return None

    def interpret(self, interpreter: Any = None) -> None:  # noqa: ANN401
        return None

    def dump(self) -> bytes:
        return pickle.dumps(self)

    def stop(self, *args, **kwargs) -> None:
        return

    def get_injectables(self) -> list[BaseInjectable]:
        return []

    def get_operations(self) -> list[str]:
        return []

    def commit_returned(self, returned: FlowOutput) -> None:
        self._returned = returned

    def results(
        self,
        run_id: Optional[str] = None,
        *args,
        **kwargs
    ) -> Iterable[SpaceCollectionResult]:
        returned = self._returned
        if returned is None:
            return None

        if isinstance(returned, traced):
            returned = [returned]

        alias2infid = self.state.space.get_snapshot_components(
            run_id or self.state.aux['run_id']
        )

        _, infid_ = self._deflate(
            returned,
            alias2infid
        )

        finished_ = set()
        to_be_finished_ = set(infid_)
        for update in self.state.space.subscribe_to_status(
            run_id or self.state.aux['run_id']
        ):
            if update.status == AppStatus.COMPLETE:
                finished_.add(update.in_flow_comp_id)

            if finished_ == to_be_finished_:
                break

        return [
            SpaceCollectionResult(
                run_id=run_id or self.state.aux['run_id'],
                in_flow_id=i,
                space_ops=self.state.space
            ) for i in infid_
        ]
