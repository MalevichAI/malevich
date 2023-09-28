
import malevich._autoflow.tracer as gn
from typing import Any, Optional, TypeVar, Callable
import pandas as pd
from uuid import uuid4
from malevich.models.collection import Collection
from malevich.models.task import Task



def collection(
    name: Optional[str] = None,
    file: Optional[str] = None,
    data: Optional[pd.DataFrame] = None
):
    assert any([file is not None, data is not None]), "Either file or data must be provided"
    if file:
        assert data is None, "Cannot provide both file and data"
        data = pd.read_csv(file)

    name = name or uuid4().hex

    from malevich.models.collection import Collection
    return gn.tracer((name, {
            '__collection__': Collection(collection_id=name, collection_data=data),
        }
    ))



def pipeline(
    interpreter: Optional[str] = None,
    core_host: Optional[str] = 'https://core.onjulius.co',
) -> Callable[[], str]:

    def wrapper(function: Callable[[], None]) -> Callable[[], str]:
        from malevich._autoflow.manager import Flow
        def fn(*args, **kwargs):  # noqa: ANN202, ANN002, ANN003
            match (interpreter):
                case 'core':
                    from malevich.interpreter.viacore import CoreInterpreter
                    __tree = None
                    with Flow() as tree:
                        function(*args, **kwargs)
                        __tree = tree
                    __interpreter = CoreInterpreter()
                    return Task(__interpreter.interpret(__tree, core_host=core_host))
                case _:
                    raise NotImplementedError(f"Interpreter {interpreter} not implemented")
        return fn

    return wrapper

