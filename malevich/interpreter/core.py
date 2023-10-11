import uuid
from collections import defaultdict
from typing import Any

import jls_utils as jls

from malevich._autoflow.tracer import tracer
from malevich._utility.registry import Registry
from malevich.constants import DEFAULT_CORE_HOST
from malevich.interpreter.abstract import Interpreter
from malevich.manifest import ManifestManager


class CoreInterpreterState:
    """State of the CoreInterpreter"""

    def __init__(self) -> None:
        # Involved operations
        self.ops: dict[str, Any] = {}
        # Dependencies (same keys as in self.ops)
        self.depends = defaultdict(list[tuple[tracer, str]])
        # Registry reference (just a shortcut)
        self.reg = Registry()
        # Manifest manager reference (just a shortcut)
        self.manf = ManifestManager()
        # Task configuration
        self.cfg = jls.Cfg()
        # Collections (key: operation_id, value: (`Collection` object, core_id,))
        self.collections: dict[str, str] = {}
        # Uploaded operations (key: operation_id, value: core_op_id)
        self.core_ops: dict[str, str] = {}
        # Interpreter parameters
        self.params: dict[str, Any] = {}


class CoreInterpreter(Interpreter[CoreInterpreterState, tuple[str, str]]):
    """Inteprets the flow using Malevich Core API"""

    def __init__(
        self, core_host: str = DEFAULT_CORE_HOST, core_auth: tuple[str, str] = None
    ) -> None:
        super().__init__(CoreInterpreterState())
        self.__core_host = core_host
        self.__core_auth = core_auth
        self._state.params["core_host"] = core_host
        self._state.params["core_auth"] = core_auth

        self.update_state()


    def create_node(
        self, state: CoreInterpreterState, tracer: tracer[tuple[str, dict]]
    ) -> CoreInterpreterState:
        state.ops[tracer.owner[0]] = {
            **state.reg.get(tracer.owner[0], {}),
            **tracer.owner[1],
            "tracer": tracer,
        }
        return state

    def create_dependency(
        self,
        state: CoreInterpreterState,
        callee: tracer[tuple[str, dict]],
        caller: tracer[tuple[str, dict]],
        link: Any,  # noqa: ANN401
    ) -> CoreInterpreterState:
        state.depends[caller.owner[0]].append((callee.owner[0], link))
        return state

    def before_interpret(self, state: CoreInterpreterState) -> CoreInterpreterState:
        jls.set_host_port(self.__core_host)
        if self.__core_auth is None:
            self.__core_auth = (uuid.uuid4().hex, uuid.uuid4().hex)
            jls.create_user(self.__core_auth)
        jls.update_core_credentials(self.__core_auth[0], self.__core_auth[1])
        state.params["core_auth"] = self.__core_auth
        return state

    def after_interpret(self, state: CoreInterpreterState) -> CoreInterpreterState:
        for id, app in state.ops.items():
            if operation_id := app.get("operation_id", None):
                __app = state.reg.get(operation_id)
                image_auth_user = state.manf.query(
                    *__app["image_auth_user"], resolve_secrets=True
                )

                image_auth_pass = state.manf.query(
                    *__app["image_auth_pass"], resolve_secrets=True
                )

                image_ref = state.manf.query(
                    *__app["image_ref"], resolve_secrets=True
                )

                extra_colls = {}

                for dependency, (link, _) in state.depends[id]:
                    if dependency in state.collections:
                        coll, __id = state.collections[dependency]
                        state.cfg.collections = {
                            **state.cfg.collections,
                            f"{coll.collection_id}": __id,
                        }
                        extra_colls[link] = coll.collection_id

                __app_id = uuid.uuid4().hex + f"-{__app['processor_id']}"
                jls.create_app(
                    app_id=__app_id,
                    processor_id=__app["processor_id"],
                    image_auth=(image_auth_user, image_auth_pass),
                    image_ref=image_ref,
                    extra_collections_from=extra_colls,
                    app_cfg=app.get("app_cfg", {}),
                )

                state.core_ops[id] = __app_id
            elif collection_ref := app.get("__collection__", None):
                if isinstance(app["tracer"], tracer):
                    __id = jls.create_collection_from_df(
                        collection_ref.collection_data,
                        collection_ref.collection_id,
                    )
                    state.collections[id] = (collection_ref, __id)
                else:
                    raise ValueError("Collection reference is not a tracer")
        return state

    def get_result(self, state: CoreInterpreterState) -> str:
        for id, core_id in sorted(
            state.core_ops.items(), key=lambda x: len(state.depends[x[0]])
        ):
            depends = state.depends[id]
            depends.sort(key=lambda x: x[1][1])
            depends = [
                state.core_ops[x[0]]
                for x in depends
                if x[0] in state.core_ops
            ]

            jls.create_task(
                task_id=core_id,
                app_id=core_id,
                tasks_depends=depends
            )

        __cfg = uuid.uuid4().hex
        jls.create_cfg(__cfg, state.cfg)
        leaves = [*self._tree.leaves()]
        return state.core_ops[leaves[0].owner[0]], __cfg
