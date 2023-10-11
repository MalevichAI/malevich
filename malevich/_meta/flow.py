from typing import Callable, Optional

from .._autoflow.manager import Flow
from ..constants import DEFAULT_CORE_HOST
from ..interpreter.core import CoreInterpreter
from ..models.platform import Platform
from ..models.task import Task


def flow(
    interpreter: Optional[Platform] = None,
    core_host: Optional[str] = DEFAULT_CORE_HOST,
    core_auth: Optional[tuple[str, str]] = None,
) -> Callable[[], tuple[Task, object]]:
    def wrapper(function: Callable[[], None]) -> Callable[[], str]:
        def fn(*args, **kwargs):  # noqa: ANN202, ANN002, ANN003
            match (interpreter):
                case Platform.CORE:
                    __tree = None
                    with Flow() as tree:
                        function(*args, **kwargs)
                        __tree = tree
                    __interpreter = CoreInterpreter(
                        core_host=core_host, core_auth=core_auth
                    )
                    __task, __cfg = __interpreter.interpret(__tree)
                    return Task(
                        alias=function.__name__,
                        task_id=__task,
                        cfg_id=__cfg,
                        host=core_host
                    ), __interpreter.state
                case _:
                    raise NotImplementedError(
                        f"Interpreter {interpreter} is not implemented"
                    )
        return fn
    return wrapper
