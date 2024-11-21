from collections import defaultdict
import uuid
import weakref
from hashlib import sha256
from typing import Any, Callable, Generic, Optional, TypeVar

from pydantic import BaseModel, Field

from .flow import Flow
from .tree import ExecutionTree


class root(BaseModel):   # noqa: N801
    """A root element of the execution tree

    Used as a default value for the owner of the traced objects
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
    """Autoflow is a bridge between traced objects and the execution tree

    It holds both the reference to the execution tree and the reference
    to the traced object. Can be reattached to another traced object. Provides
    interfaces for reporting a new dependency in the execution tree.
    """

    _trace_map: dict[weakref.ref[ExecutionTree], set['autoflow']] = defaultdict(set)
    _tracers = 0

    @staticmethod
    def get_tree_ref(tree: ExecutionTree) -> weakref.ref[ExecutionTree] | None:
        for t in autoflow._trace_map.keys():
            if t() is not None and t() is tree:
                return t
        return weakref.ref(tree)

    @staticmethod
    def get_trace_refs(tree: ExecutionTree) -> set['autoflow']:
        tree = autoflow.get_tree_ref(tree)
        if tree is None:
            raise ValueError("Tree not found")
        return autoflow._trace_map[tree]


    @staticmethod
    def retrace(old_tree: ExecutionTree, new_tree: ExecutionTree, /) -> None:
        new_tree_ref = autoflow.get_tree_ref(new_tree)
        for a in autoflow.get_trace_refs(old_tree):
            a._tree_ref = new_tree_ref
            a._notify_spectators(old_tree, new_tree)
            autoflow._trace_map[new_tree_ref].add(a)

    def retrace_self(self, new_tree: ExecutionTree) -> 'autoflow':
        new_tree_ref = autoflow.get_tree_ref(new_tree)

        autoflow._trace_map[self._tree_ref].remove(self)
        self._tree_ref = new_tree_ref
        self._notify_spectators(self._tree_ref(), new_tree)
        autoflow._trace_map[new_tree_ref].add(self)
        return self

    def __init__(self, tree: ExecutionTree[T, Any]) -> None:
        self._tree_ref = autoflow.get_tree_ref(tree)
        self._component_ref = None
        self._trace_map[self._tree_ref].add(self)
        self._tracers += 1
        self._spectators = []

    def attach(self, component: T) -> None:
        """Attach the autoflow to a traced object"""
        self._component_ref = component

    def _notify_spectators(self, old_tree: ExecutionTree, new_tree: ExecutionTree):
        for spectator in self._spectators:
            spectator(self, old_tree, new_tree)

    def spectate_retrace(
        self, callback: Callable[['autoflow', ExecutionTree, ExecutionTree], None]
    ):
        self._spectators.append(callback)

    def calledby(self, caller: 'traced', argument: Optional[str] = None) -> None:
        """Report a new dependency in the execution tree"""
        assert isinstance(caller, traced), "Caller must be a traced object"
        self._tree_ref().put_edge(self._component_ref, caller, argument)

    def __deepcopy__(self, *args):
        copy = super().__deepcopy__(*args)
        autoflow._trace_map[copy._tree_ref].add(copy)
        self._tracers += 1
        return copy

    def __copy__(self, *args):
        copy = super().__copy__(*args)
        autoflow._trace_map[copy._tree_ref].add(copy)
        self._tracers += 1
        return copy



class traced(Generic[T]):  # noqa: N801
    """
    Traced objects are wrappers around regular Python objects
    They hold a reference to the current execution tree and
    provides an access to :class:`autoflow` to enable dependency tracking

    These objects CAN ONLY BE CREATED inside a flow.
    """

    _autoflow: autoflow
    """Autoflow bridge"""

    __keep_tree_on_deepcopy__ = True

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

    def claim(self, owner: T) -> None:
        """Claim the ownership of the tracer"""
        self._owner = owner

    @property
    def owner(self) -> T:
        if hasattr(self, '_owner'):
            return getattr(self, '_owner')
        else:
            return None

    def __eq__(self, other: "traced") -> bool:
        if not isinstance(other, traced):
            return False
        return self.owner == other.owner

    def __str__(self) -> str:
        if not isinstance(self.owner, traced):
            return f'{self.owner.__str__()}ᵗ'
        else:
            raise RuntimeError(
                "Traced object is not supposed to be nested"
            )

    def __repr__(self) -> str:
        if not isinstance(self.owner, traced):
            return f'traced({self.owner.__repr__()})'
        else:
            raise RuntimeError(
                "Traced object is not supposed to be nested"
            )

    def __hash__(self) -> int:
        return hash(self.owner)

    def __copy__(self, *args):
        return self

    def __deepcopy__(self, *args):
        return self

    def __getitem__(self, *args):
        return tracedLike(self._owner.__getitem__(*args))


class tracedLike(traced[T]):  # noqa: N801
    """This class is used to immitate the traced object without actually tracing it

    It is used to provide homogeneous interface for tree nodes
    without attaching them to a particular tree.

    These objects CAN BE CREATED from outside of a flow.
    """
    def __init__(self, owner: T = root()) -> None:
        self._owner = owner

    @property
    def _autoflow(self) -> autoflow:
        raise ValueError(
            "Tracedlike object does not have an autoflow. Use `traced` instead."
        )

    @property
    def owner(self) -> T:
        return self._owner

    def claim(self, owner: T) -> None:
        return super().claim(owner)

    def __str__(self) -> str:
        return super().__str__() + "*"

    def __repr__(self) -> str:
        return super().__repr__() + "*"
