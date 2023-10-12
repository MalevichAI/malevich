import uuid
from hashlib import sha256
from typing import Any, Generic, Iterable, TypeVar, Union

from pydantic import BaseModel, Field

from .manager import Flow
from .tree import ExecutionTree


class root(BaseModel):   # noqa: N801
    """A root element of the execution tree

    Used as a default value for the owner of the tracer
    """

    id: str = Field(..., default_factory=lambda: uuid.uuid4().hex)

    def __eq__(self, other: "root") -> bool:
        if not isinstance(other, root):
            return False
        return self.id == other.id


    def __hash__(self) -> int:
        return hash(sha256(self.id.encode()).hexdigest)


T = TypeVar("T", bound=Any)
class autoflow(Generic[T]):  # noqa: N801
    """Autoflow is a bridge between the tracer and the execution tree"""
    def __init__(self, tree: ExecutionTree[T]) -> None:
        self._tree_ref = tree
        self._component_ref = None

    def attach(self, component: T) -> None:
        self._component_ref = component

    def calledby(self, caller: 'tracer', argument: str = None) -> None:
        self._tree_ref.put_edge(self._component_ref, caller, argument)


class tracer(Generic[T]):  # noqa: N801
    """Tracer is a special object that is used to track the execution of the pipeline"""
    def __init__(
        self,
        owner: T = root()
    ) -> None:
        assert (
            Flow.isinflow()
        ), "Tried to create a tracked variable outside of a flow"
        self._owner = owner
        self._autoflow = autoflow(Flow.flow_ref())
        self._autoflow.attach(self)

    @property
    def owner(self) -> T:
        return self._owner



class multitracer(tracer, list[T], Generic[T]):   # noqa: N801
    """Multitracer is a tracer for collections"""
    def __init__(
        self,
        owner: T = root(),
        __iterable: Iterable[T] = []
    ) -> None:
        super().__init__(__iterable)
        super().__init__(owner)


def trace(x: T) -> Union[tracer[T], T]:
    """Create a hidden tracer for the given object"""
    if isinstance(x, list):
        return multitracer.__init__(x)
    else:
        tracer.__init__(x)
    return x
