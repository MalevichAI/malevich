import json
import os
import uuid
from collections import defaultdict
from typing import Any, Iterable

import malevich_coretools as core
import pandas as pd

from .._autoflow.tracer import traced
from .._utility.cache.manager import CacheManager
from .._utility.registry import Registry
from ..constants import DEFAULT_CORE_HOST
from ..interpreter.abstract import Interpreter
from ..manifest import ManifestManager
from ..models.collection import Collection
from ..models.exceptions import InterpretationError
from ..models.nodes import BaseNode, CollectionNode, OperationNode, TreeNode
from ..models.task.interpreted import InterpretedTask

cache = CacheManager()


class CoreInterpreterState:
    """State of the CoreInterpreter"""

    def __init__(self) -> None:
        # Involved operations
        self.ops: dict[str, BaseNode] = {}
        # Dependencies (same keys as in self.ops)
        self.depends: dict[str, list[tuple[BaseNode, tuple[str, int]]]] = defaultdict(lambda: [])  # noqa: E501
        # Registry reference (just a shortcut)
        self.reg = Registry()
        # Manifest manager reference (just a shortcut)
        self.manf = ManifestManager()
        # Task configuration
        self.cfg = core.Cfg()
        # Collections (key: operation_id, value: (`Collection` object, core_id,))
        self.collections: dict[str, tuple[Collection, str]] = {}
        # Uploaded operations (key: operation_id, value: core_op_id)
        self.core_ops: dict[str, BaseNode] = {}
        # Interpreter parameters
        self.params: dict[str, Any] = {}
        # Results
        self.results: dict[str, str] = {}
        # Interpretation ID
        self.interpretation_id: str = uuid.uuid4().hex


class CoreInterpreter(Interpreter[CoreInterpreterState, tuple[str, str]]):
    """Inteprets the flow using Malevich Core API"""

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
        return f"result-{operation_id}"

    def _write_cache(self, object: object, path: str) -> None:
        json.dump(object, cache.get_file(os.path.join('core', path), 'w+'))

    def _create_app_safe(
            self, app_id: str, extra: dict, uid: str, *args, **kwargs
    ) -> None:

        # TODO: Wrap correctly
        try:
            core.create_app(
                app_id,
                processor_id=extra["processor_id"],
                *args,
                **kwargs
            )
        except Exception:
            pass
        else:
            return

        try:
            if core.get_app(app_id):
                core.delete_app(app_id)
                core.delete_task(app_id)
        except Exception:
            pass

        try:
            core.create_app(
                app_id,
                processor_id=extra["processor_id"],
                *args,
                **kwargs
            )
        except Exception as e:
            if 'processor_id' in extra:
                processor_id = extra['processor_id']
                raise InterpretationError(
                    f"Failed to create an app. Processor is {processor_id}. "
                ) from e
            else:
                raise InterpretationError(
                    "Failed to create an app and could determine the processor. "
                    "Most probably, the app is installed incorrectly. Use "
                    "malevich remove to remove it and reinstall it correctly"
                ) from e

        kwargs = {
            "app_id": app_id,
            "app_cfg": kwargs['app_cfg'],
            "image_ref": kwargs['image_ref'],
            "extra_collections_from": kwargs['extra_collections_from'],
        }

        self._write_cache(
            kwargs, f"app-{app_id}-{self.state.interpretation_id}.json")

        return core.AppSettings(
            appId=app_id,
            taskId=app_id,
            saveCollectionsName=[self._result_collection_name(app_id)],
        )

    def create_node(
        self, state: CoreInterpreterState, node: traced[BaseNode]
    ) -> CoreInterpreterState:
        state.ops[node.owner.uuid] = node.owner
        return state

    def create_dependency(
        self,
        state: CoreInterpreterState,
        callee: traced[BaseNode],
        caller: traced[BaseNode],
        link: Any,  # noqa: ANN401
    ) -> CoreInterpreterState:
        state.depends[caller.owner.uuid].append((callee.owner, link))
        return state

    def before_interpret(self, state: CoreInterpreterState) -> CoreInterpreterState:
        core.set_host_port(self.__core_host)
        core.update_core_credentials(self.__core_auth[0], self.__core_auth[1])
        return state

    def after_interpret(self, state: CoreInterpreterState) -> CoreInterpreterState:
        for id, op in state.ops.items():
            if isinstance(op, OperationNode):
                extra = state.reg.get(op.operation_id, {})
                image_auth_user = state.manf.query(
                    *extra["image_auth_user"],
                    resolve_secrets=True
                )

                image_auth_pass = state.manf.query(
                    *extra["image_auth_pass"],
                    resolve_secrets=True
                )

                image_ref = state.manf.query(
                    *extra["image_ref"],
                    resolve_secrets=True
                )

                extra_colls = {}

                for node, (link, _) in state.depends[id]:
                    if isinstance(node, CollectionNode) and \
                            node.uuid in state.collections:
                        coll, uploaded_core_id = state.collections[node.uuid]
                        state.cfg.collections = {
                            **state.cfg.collections,
                            f"{coll.collection_id}": uploaded_core_id,
                        }
                        extra_colls[link] = coll.collection_id

                app_core_name = uuid.uuid4().hex + f"-{extra['processor_id']}"

                state.core_ops[op.uuid] = app_core_name
                state.cfg.app_settings.append(
                    self._create_app_safe(
                        app_id=app_core_name,
                        extra=extra,
                        image_auth=(image_auth_user, image_auth_pass),
                        image_ref=image_ref,
                        extra_collections_from=extra_colls,
                        app_cfg=op.config,
                        uid=op.uuid,
                    )
                )
                state.results[op.uuid] = self._result_collection_name(op.uuid)

            elif isinstance(op, CollectionNode):
                collection_ref = op.collection

                uploaded_core_id = core.create_collection_from_df(
                    collection_ref.collection_data,
                    collection_ref.collection_id,
                )

                state.collections[op.uuid] = (collection_ref, uploaded_core_id)
        return state

    def get_task(
        self, state: CoreInterpreterState
    ) -> InterpretedTask[CoreInterpreterState]:
        task_kwargs = []
        config_kwargs = []

        for id, core_id in sorted(
            state.core_ops.items(), key=lambda x: len(state.depends[x[0]])
        ):
            depends = state.depends[id]
            depends.sort(key=lambda x: x[1][1])
            depends = [
                state.core_ops[x[0].uuid]
                for x in depends
                if x[0].uuid in state.core_ops
            ]

            task_kwargs.append(
                {
                    'task_id': core_id,
                    'app_id': core_id,
                    'tasks_depends': depends
                }
            )

            self._write_cache(
                task_kwargs[-1],
                f'task-{core_id}-{self.state.interpretation_id}.json'
            )

        config_kwargs = {
            'cfg_id': uuid.uuid4().hex,
            'cfg': state.cfg,
        }

        __cfg = uuid.uuid4().hex
        core.create_cfg(__cfg, state.cfg)
        leaves = [*self._tree.leaves()]

        def prepare(
            task: InterpretedTask[CoreInterpreterState], *args, **kwargs
        ) -> None:
            for _kwargs in task_kwargs:
                core.create_task(**_kwargs)

            core.create_cfg(**config_kwargs)

            task_id = task.state.core_ops[leaves[0].owner.uuid]
            try:
                task.state.params["task_id"] = core.task_prepare(
                    task_id=task_id,
                    cfg_id=config_kwargs['cfg_id'],
                    *args,
                    **kwargs
                ).operationId
            except Exception as e:
                # Cleanup
                core.task_stop(task_id)
                raise e

        def run(task: InterpretedTask[CoreInterpreterState], *args, **kwargs) -> None:
            if "task_id" not in task.state.params:
                raise Exception("Attempt to run a task which is not prepared. "
                                "Please, run `.prepare()` first.")
            try:
                core.task_run(task.state.params["task_id"], *args, **kwargs)
            except Exception as e:
                # Cleanup
                core.task_stop(task.state.params["task_id"])
                raise e

        def stop(
            task: InterpretedTask[CoreInterpreterState],
            *args,
            **kwargs
        ) -> None:
            if "task_id" not in task.state.params:
                raise Exception("Attempt to run a task which is not prepared. "
                                "Please, run `.prepare()` first.")
            core.task_stop(task.state.params["task_id"], *args, **kwargs)

        def results(
            task: InterpretedTask[CoreInterpreterState],
            returned: Iterable[traced[BaseNode]] | traced[BaseNode] | None,
            *args,
            **kwargs
        ) -> Iterable[pd.DataFrame] | pd.DataFrame:
            if "task_id" not in task.state.params:
                raise Exception("Attempt to run a task which is not prepared. "
                                "Please, run `.prepare()` first.")
            return self.get_results(returned)

        return InterpretedTask(
            prepare=prepare,
            run=run,
            stop=stop,
            results=results,
            state=state,
        )

    def get_results(
        self,
        returned: Iterable[traced[BaseNode]] | traced[BaseNode] | None
    ) -> Iterable[pd.DataFrame] | pd.DataFrame:
        if not returned:
            return None

        if isinstance(returned, traced):
            returned = [returned]

        results = []
        for r in returned:
            node = r.owner
            if isinstance(node, CollectionNode):
                results.append(node.collection.collection_data)
            elif isinstance(node, OperationNode):
                collection_ids = core.get_collections_by_name(
                    self._result_collection_name(node.uuid)
                ).ownIds

                results.append(core.get_collection_to_df(
                    collection_ids[-1]
                ))
            elif isinstance(node, TreeNode):
                results.append(self.get_results(node.results))
        return results[0] if len(results) == 1 else results
