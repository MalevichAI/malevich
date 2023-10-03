import uuid
from collections import defaultdict
from typing import Any

import jls_utils as jls
from pydantic import BaseModel

from malevich._autoflow.tracer import tracer
from malevich._autoflow.tree import ExecutionTree
from malevich._utility.registry import Registry
from malevich.constants import DEFAULT_CORE_HOST
from malevich.interpreter.abstract import Interpreter
from malevich.manifest import ManifestManager


class CoreInterpreter(Interpreter):
    """Inteprets the flow using Malevich Core API"""

    def interpret(
        self,
        tree: ExecutionTree[tracer],
        core_auth: str = None,
        core_host: str = DEFAULT_CORE_HOST
    ) -> str:
        """Interprets the given execution tree using Malevich Core API

        Args:
            tree (ExecutionTree[tracer]): Execution tree to interpret
            core_auth (str, optional): Core authentication. Defaults to None.
            core_host (str, optional): Core host. Defaults to DEFAULT_CORE_HOST.

        Returns:
            str: Operation ID
        """
        jls.set_host_port(core_host)
        if core_auth is None:
            core_auth = (uuid.uuid4().hex, uuid.uuid4().hex)
            jls.create_user(core_auth)
        jls.update_core_credentials(core_auth[0], core_auth[1])
        # `apps` is mapping from traced instance_id
        # to the operation
        apps: dict[str, Any] = {}

        # `collections` is mapping from traced instance_id
        # to the collection
        collections: dict[str, str] = {}

        # `depends` is mapping from traced instance_id
        depends = defaultdict(list[tuple[tracer, str]])
        reg = Registry()
        manf = ManifestManager()
        cfg = jls.Cfg()

        # tree edge is a tuple of
        # 1. traced tuple of (instance_id, operation_info)
        # 2. traced tuple of (instance_id, operation_info)
        # 3. link infow
        for left, right, link in tree.traverse():
            # left instance reference
            li_ref = left.owner[0]
            # right instance reference
            ri_ref = right.owner[0]
            if li_ref not in apps:
                data = reg.get(li_ref, {})
                apps[li_ref] = {**data, **left.owner[1], "tracer": left}
            if ri_ref not in apps:
                data = reg.get(ri_ref, {})
                apps[ri_ref] = {**data, **right.owner[1], "tracer": right}
            depends[ri_ref].append((li_ref, link))

        core_apps = {}
        for id, app in apps.items():
            if operation_id := app.get("operation_id", None):
                __app = reg.get(operation_id)
                image_auth_user = manf.query(
                    *__app["image_auth_user"], resolve_secrets=True
                ).secret_value
                image_auth_pass = manf.query(
                    *__app["image_auth_pass"], resolve_secrets=True
                ).secret_value
                image_ref = manf.query(
                    *__app["image_ref"], resolve_secrets=True
                ).secret_value

                extra_colls = {}
                for d, link in depends[id]:
                    if d in collections:
                        coll, __id = collections[d]
                        cfg.collections = {
                            **cfg.collections,
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
                core_apps[id] = __app_id
            elif collection_ref := app.get("__collection__", None):
                if isinstance(app["tracer"], tracer):
                    __id = jls.create_collection_from_df(
                        collection_ref.collection_data,
                        collection_ref.collection_id,
                    )
                    collections[id] = (collection_ref, __id)
                else:
                    raise ValueError("Collection reference is not a tracer")

        for id, core_id in sorted(core_apps.items(), key=lambda x: len(depends[x[0]])):
            jls.create_task(
                task_id=core_id,
                app_id=core_id,
                tasks_depends=[
                    core_apps[x[0]] for x in depends[id] if x[0] in core_apps
                ],
            )

        __cfg = uuid.uuid4().hex
        jls.create_cfg(__cfg, cfg)
        leaves = [*tree.leaves()]
        return jls.task_prepare(core_apps[leaves[0].owner[0]], __cfg).operationId

    def get_model(self) -> BaseModel:
        return None
