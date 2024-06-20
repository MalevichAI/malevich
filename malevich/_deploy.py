from typing import Any, Literal, ParamSpec, overload

from malevich_space.ops import SpaceOps
from malevich_space.schema import SpaceSetup
from malevich_space.schema.version_mode import VersionMode
from rich.prompt import Prompt

from malevich.core_api import check_auth

from ._cli.space.login import login
from ._utility.space.get_core_creds import (
    get_core_creds_from_db,
    get_core_creds_from_setup,
)
from .constants import DEFAULT_CORE_HOST
from .interpreter.core import CoreInterpreter
from .interpreter.core_v2 import CoreInterpreterV2
from .interpreter.space import SpaceInterpreter
from .manifest import manf
from .models.flow_function import FlowFunction
from .models.task.interpreted.core import CoreTask
from .models.task.interpreted.core_v2 import CoreTaskV2
from .models.task.interpreted.space import SpaceTask
from .models.task.promised import PromisedTask

FlowArgs = ParamSpec('FlowArgs')

class Core:
    @overload
    def __new__(
        cls,
        task: PromisedTask | FlowFunction[..., PromisedTask] | Any,  # noqa: ANN401, for IDE hints
        core_host: str | None = DEFAULT_CORE_HOST,
        user: str | None = None,
        access_key: str | None = None,
        use_v2: Literal[True] = True,
        *task_args,
        **task_kwargs
    ) -> CoreTaskV2:
        pass

    @overload
    def __new__(
        cls,
        task: PromisedTask | FlowFunction[..., PromisedTask] | Any,  # noqa: ANN401, for IDE hints
        core_host: str | None = DEFAULT_CORE_HOST,
        user: str | None = None,
        access_key: str | None = None,
        use_v2: Literal[False] = False,
        *task_args,
        **task_kwargs
    ) -> CoreTask:
        pass

    def __new__(
        cls,
        task: PromisedTask | FlowFunction[..., PromisedTask] | Any,
        core_host: str | None = DEFAULT_CORE_HOST,
        user: str | None = None,
        access_key: str | None = None,
        use_v2: Literal[True] = False,
        *task_args,
        **task_kwargs
    ) -> CoreTask | CoreTaskV2:
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

        interpreter_class = (CoreInterpreter, CoreInterpreterV2)[use_v2]
        intepreter = interpreter_class(
            core_auth=(user, access_key), core_host=core_host
        )
        task.interpret(intepreter)
        return task.get_interpreted_task()


class Space:

    def __new__(
        cls,
        task: PromisedTask | FlowFunction[..., Any] | Any = None,  # noqa: ANN401, for IDE hints
        version_mode: VersionMode = VersionMode.MINOR,
        reverse_id: str | None = None,
        attach_to_any: bool = False,
        deployment_id: str | None = None,
        attach_to_last: bool | None = None,
        branch: str | None = None,
        ops: SpaceOps | None = None,
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

        interpreter = SpaceInterpreter(
            setup=setup,
            ops=ops,
            version_mode=version_mode
        )

        if isinstance(task, FlowFunction):
            task: PromisedTask = task(*task_args, **task_kwargs)

        if isinstance(task, PromisedTask):
            reverse_id = task._component.reverse_id

        if attach_to_any or task is None:
            return interpreter.attach(
                reverse_id=reverse_id,
                deployment_id=deployment_id,
                attach_to_last=attach_to_last,
                branch=branch,
                attach_to_any=attach_to_any
            )

        task.interpret(interpreter)
        return task.get_interpreted_task()

