
from enum import Enum
from typing import Callable, Optional
from uuid import uuid4

import pandas as pd

import malevich._autoflow.tracer as gn
from malevich._autoflow.manager import Flow
from malevich.constants import DEFAULT_CORE_HOST
from malevich.interpreter.core import CoreInterpreter
from malevich.models.collection import Collection
from malevich.models.task import Task


class Platform(Enum):
  CORE = "CORE"
  SPACE = "SPACE"


Core = Platform.CORE
Space = Platform.SPACE


def collection(
    name: Optional[str] = None,
    file: Optional[str] = None,
    data: Optional[pd.DataFrame] = None
) -> Collection:
    """Creates a collection from a file or a dataframe

    Args:
        name (Optional[str], optional): Name of the collection. Defaults to None.
        file (Optional[str], optional): Path to the file. Defaults to None.
        data (Optional[pd.DataFrame], optional): Dataframe. Defaults to None.

    Raises:
        AssertionError: If both file and data are provided

    Returns:
        Collection: an object that is subject to be passed to processors
    """
    assert any([file is not None, data is not None]), \
        "Either file or data must be provided"

    if file:
        assert data is None, "Cannot provide both file and data"
        data = pd.read_csv(file)

    name = name or uuid4().hex

    return gn.tracer((name, {
            '__collection__': Collection(collection_id=name, collection_data=data),
        }
    ))



def flow(
    interpreter: Optional[Platform] = None,
    core_host: Optional[str] = DEFAULT_CORE_HOST
) -> Callable[[], str]:

    def wrapper(function: Callable[[], None]) -> Callable[[], str]:
        def fn(*args, **kwargs):  # noqa: ANN202, ANN002, ANN003
            match (interpreter):
                case Platform.CORE:
                    __tree = None
                    with Flow() as tree:
                        function(*args, **kwargs)
                        __tree = tree
                    __interpreter = CoreInterpreter(core_host=core_host)
                    __task, __cfg = __interpreter.interpret(__tree)
                    return Task(task_id=__task, cfg=__cfg)
                case _:
                    raise NotImplementedError(
                        f"Interpreter {interpreter} is not implemented"
                    )
        return fn

    return wrapper

