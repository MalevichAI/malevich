from typing import Any, Literal, ParamSpec

from malevich_space.ops import SpaceOps
from malevich_space.schema import SpaceSetup
from rich.prompt import Prompt

from malevich.core_api import check_auth

from ._cli.space.login import login
from ._utility.space.auto_space_ops import get_auto_ops
from ._utility.space.get_core_creds import (
    get_core_creds_from_db,
    get_core_creds_from_setup,
)
from .constants import DEFAULT_CORE_HOST
from .interpreter.core import CoreInterpreter
from .interpreter.space import SpaceInterpreter
from .manifest import manf
from .models.flow_function import FlowFunction
from .models.task.interpreted.core import CoreTask
from .models.task.interpreted.space import SpaceTask
from .models.task.promised import PromisedTask

FlowArgs = ParamSpec('FlowArgs')

class Core:
    def __new__(
        cls,
        task: PromisedTask | FlowFunction[..., PromisedTask] | Any,  # noqa: ANN401
        core_host: str | None = DEFAULT_CORE_HOST,
        user: str | None = None,
        access_key: str | None = None,
        *task_args,
        **task_kwargs
    ) -> CoreTask:
        if not user:
            try:
                user, access_key = get_core_creds_from_setup(
                    SpaceSetup(**manf.query('space', resolve_secrets=True))
                )
            except Exception:
                from malevich._cli.space.login import login
                if login():
                    user, access_key = get_core_creds_from_setup(
                        SpaceSetup(**manf.query('space', resolve_secrets=True))
                    )
                else:
                    try:
                        user, access_key = get_core_creds_from_db(user, core_host)
                    except Exception:
                        # TODO: Add Follow: {link}
                        raise ValueError("No user found. Use `Core(user=...)`")

        if not access_key:
            access_key = Prompt.ask(f"Password for {user} on Core:", password=True)

        try:
            check_auth(conn_url=core_host, auth=(user, access_key,))
        except Exception:
            raise

        if isinstance(task, FlowFunction):
            task: PromisedTask = task(*task_args, **task_kwargs)

        intepreter = CoreInterpreter(
            core_auth=(user, access_key), core_host=core_host
        )
        task.interpret(intepreter)
        return task.get_interpreted_task()


class Space:

    @staticmethod
    def fetch(reverse_id: str, ops: SpaceOps | None = None) -> tuple[dict, str, dict[str, str]]:  # noqa: E501
        if not ops:
            ops = get_auto_ops()

        info = ops.get_available_flows(reverse_id=reverse_id)
        flow_branch_version = {}
        active_branch = info['component']['activeBranch']['details']['name']
        active_versions = {}
        for branch_ in info['component']['branches']['edges']:
            branch_name = branch_['node']['details']['name']
            active_versions[branch_name] = branch_['node']['activeVersion']['flow']['details']['uid']  # noqa: E501
            flow_branch_version[branch_name] = {}
            for version_ in branch_['node']['versions']['edges']:
                version_name = version_['node']['details']['readableName']
                flow_branch_version[branch_name][version_name] = version_['node']['flow']['details']['uid']  # noqa: E501

        return flow_branch_version, active_branch, active_versions

    def __new__(
        cls,
        task: PromisedTask | FlowFunction[..., Any] | Any = None,  # noqa: ANN401, for IDE hints
        # version_mode: VersionMode = VersionMode.MINOR,
        reverse_id: str | None = None,
        uid: str | None = None,
        deployment_id: str | None = None,
        branch: str | None = None,
        version: str | None = None,
        ops: SpaceOps | None = None,
        policy: Literal['no_use', 'only_use', 'use_or_new']= 'use_or_new',
        *task_args,
        **task_kwargs
    ) -> SpaceTask:

        setup = None
        if not ops:
            try:
                setup = SpaceSetup(**manf.query('space', resolve_secrets=True))
            except Exception:
                if not login():
                    raise Exception(
                        "Could not login you. Please, run `malevich space login` "
                        "manually and provide correct credentials."
                    )
                setup = SpaceSetup(**manf.query('space', resolve_secrets=True))
        else:
            setup = ops.space_setup

        ops = SpaceOps(space_setup=setup)


        if not uid and reverse_id is not None:
            flow_branch_version, active_branch, active_versions = cls.fetch(
                reverse_id=reverse_id,
                ops=ops
            )

            if branch is not None:
                if branch not in flow_branch_version:
                    raise ValueError(
                        f"Branch {branch} not found. Available branches: "
                        f" {list(flow_branch_version.keys())}")
            else:
                branch = active_branch

            if version is not None:
                if version not in flow_branch_version[branch]:
                    raise ValueError(
                        f"Version {version} not found in branch {branch}. "
                        f"Available versions for branch {branch}: "
                        f" {list(flow_branch_version[branch].keys())}"
                    )
                else:
                    uid = flow_branch_version[branch][version]
            else:
                uid = active_versions[branch]

        interpreter = SpaceInterpreter(
            setup=setup,
            ops=ops,
            # version_mode=version_mode
        )

        if isinstance(task, FlowFunction):
            task: PromisedTask = task(*task_args, **task_kwargs)

        if isinstance(task, PromisedTask):
            reverse_id = task._component.reverse_id

        if task is None:
            task = interpreter.attach(
                reverse_id=reverse_id,
                flow_uid=uid,
                deployment_id=deployment_id,
                search_deployments=(policy != 'no_use')
            )
            if policy == 'no_use':
                task.prepare()
                return task
            elif task.get_stage().value != 'started':
                if policy == 'only_use':
                    if deployment_id is not None:
                        raise Exception(
                            f"The deployment with ID {deployment_id} is not active "
                            "while policy was set to 'use_only'. "
                            "Provide active deployment, "
                            "or change policy to 'use_or_new' or 'no_use'"
                        )
                    else:
                        raise Exception(
                            "No active tasks found for 'use_only' policy. "
                            "You can change policy 'use_or_new' or 'no_use'."
                        )
                else:
                    task.prepare()
            return task

        task.interpret(interpreter)
        return task.get_interpreted_task()

