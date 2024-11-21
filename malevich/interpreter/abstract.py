import uuid
from abc import abstractmethod
from copy import deepcopy
from typing import Generic, TypeVar

from malevich_space.schema import ComponentSchema

from malevich._autoflow.tracer import traced
from malevich._utility import unwrap_tree
from malevich.models import ArgumentLink, BaseNode, InterpretationError, TreeNode

State = TypeVar("State")
Return = TypeVar("Return")


class Interpreter(Generic[State, Return]):
    """Base class for Interpreters

    An interpreter has an inner state, which is updated during the interpretation
    process. The state is updated by calling `update_state` method.

    The state is preserved in the history, which can be accessed by `history` property.
    Each time the state is updated, it is fully copied and appended to the history.

    The interpretation process can be divided into 5 steps:
    1. `before_interpret` - called before the interpretation process starts
    2. `create_node` - called when a new node is created
    3. `create_dependency` - called when a new dependency is created
    4. `after_interpret` - called after the interpretation process ends
    5. `get_result` - called to get the result of the interpretation process

    The base interpreter does not implement any of these steps. It is up to the
    child classes to implement them.

    But the base interpreter does implement the `interpret` method, which calls
    all the steps in the right order. It traverses the execution tree and calls
    the steps for each node and dependency. Traversing is preceeded by calling
    `before_interpret` and followed by calling `after_interpret`. The result of
    the interpretation process is returned by `get_result`.

    To implement a new interpreter, you need to inherit from this class and
    follow these guidelines:

    1.  Put any pipeline initialization logic into `before_interpret` method. For
        example intializiation of state, registry objects, establishing connections
        and so on.

    2.  The interpreter will traverse the execution tree and call `create_node` for
        each node and `create_dependency` for each dependency. The `create_node` method
        will be called twice for each node: first for the callee and then for the caller
        The `create_dependency` method will be called once for each dependency.

    3.  Put any pipeline finalization logic into `after_interpret` method. For example
        closing connections, saving state and so on.

    4. The result of the interpretation process is returned by `get_result` method.
        For example, you can return the state, or some part of it, or some other object.


    IMPORTANT: Ensure that the state is immutable. The state is copied and appended
    to the history each time it is updated. If the state is mutable, it will be
    changed in the history as well. This will lead to unexpected behavior.

    IMPORTANT: Ensure that ALL stages do return the state object if it is changed. The
    results of each stage are used as input for the next stage. If the state is not
    returned, the next stage will receive the old state.
    """

    supports_subtrees = False

    def __init__(self, initial_state: State = None) -> None:
        """Initializes the interpreter

        Args:
            initial_state (State, optional): Initial state. Defaults to None.
        """
        self._state = initial_state
        self.__history = []
        self._tree = None
        self._run_bank = []
        self._component = None

    def _get_run_id(self) -> str:
        _id = uuid.uuid4().hex
        self._run_bank.append(_id)
        return _id

    @property
    def state(self) -> State:
        """Returns the current state

        Returns:
            State: Copy of the current state
        """
        return deepcopy(self._state)

    def update_state(self, state: State = None) -> None:
        """Updates the state

        Args:
            state (State, optional): State to update.
            If not provided, the current state is used. Defaults to None.
        """
        self.__history.append(self.state)
        if state is not None:
            self._state = deepcopy(state)
        else:
            self._state = self.state

    def interpret(self, node: TreeNode, component: ComponentSchema = None) -> Return:
        """Interprets the execution tree

        The interpretation process is divided into 5 steps:
        1. `before_interpret` - called before the interpretation process starts
        2. `create_node` - called when a new node is created
        3. `create_dependency` - called when a new dependency is created
        4. `after_interpret` - called after the interpretation process ends
        5. `get_task` - called to get the executable of the interpretation process

        Before starting of the interpretation the interpreted tree is saved in the
        `_tree` property.

        Args:
            node (TreeNode): Execution tree to interpret

        Returns:
            Task: Executable result of the interpretation
        """
        # Setting the current interpreter
        setattr(node, "__interpreter__", self)
        self._component = component

        if not self.supports_subtrees:
            self._tree = unwrap_tree(node.tree)
        else:
            self._tree = node.tree

        self.update_state(self.before_interpret(self.state))

        node_memory = {}
        node_aliases = set()
        for node_ in self._tree.nodes():

            if node_.owner.alias is not None and node_.owner.alias in node_aliases:
                raise InterpretationError(
                    "Aliases should be unique, but found duplicate alias: "
                    f"{node.alias}"
                )

            if node_.owner.uuid not in node_memory:
                node_memory[node_.owner.uuid] = node_
                self.update_state(self.create_node(self.state, node_))


        for from_, to, link in self._tree.traverse():
            if from_.owner.uuid not in node_memory:
                node_memory[from_.owner.uuid] = from_
                self.update_state(self.create_node(self.state, from_))

            if to.owner.uuid not in node_memory:
                node_memory[to.owner.uuid] = to
                self.update_state(self.create_node(self.state, to))

            self.update_state(self.create_dependency(
                self.state, from_, to, link)
            )

        self.update_state(self.after_interpret(self.state))

        return self.get_task(self.state)

    @abstractmethod
    def before_interpret(self, state: State) -> State:
        """Called before the interpretation process starts

        IMPORTANT: Ensure that the state is immutable. The state is copied and appended
        to the history each time it is updated. If the state is mutable, it will be
        changed in the history as well. This will lead to unexpected behavior.

        IMPORTANT: Ensure that the method returns the state object if it is changed. The
        results of this method are used as input for the next method.

        Args:
            state (State): Current state

        Returns:
            State: Updated state
        """
        pass

    @abstractmethod
    def create_node(
        self,
        state: State,
        tracer: traced[BaseNode]
    ) -> State:
        """Called when a new node is created

        IMPORTANT: Ensure that the state is immutable. The state is copied and appended
        to the history each time it is updated. If the state is mutable, it will be
        changed in the history as well. This will lead to unexpected behavior.

        IMPORTANT: Ensure that the method returns the state object if it is changed. The
        results of this method are used as input for the next method.

        Args:
            state (State): Current state
            tracer (tracer): A node object wrapped in a tracer object

        Returns:
            State: Updated state
        """
        pass

    @abstractmethod
    def create_dependency(
        self,
        state: State,
        from_node: traced[BaseNode],
        to_node: traced[BaseNode],
        link: ArgumentLink[BaseNode],
    ) -> State:
        """Called when a new dependency is created

        IMPORTANT: Ensure that the state is immutable. The state is copied and appended
        to the history each time it is updated. If the state is mutable, it will be
        changed in the history as well. This will lead to unexpected behavior.

        IMPORTANT: Ensure that the method returns the state object if it is changed. The
        results of this method are used as input for the next method.

        Args:
            state (State): Current state
            callee (tracer): A source node object wrapped in a tracer object
            caller (tracer): A destination node object wrapped in a tracer object
            link (Any): Dependency link

        Returns:
            State: Updated state
        """
        pass

    @abstractmethod
    def after_interpret(self, state: State) -> State:
        """Called after the interpretation process ends

        IMPORTANT: Ensure that the state is immutable. The state is copied and appended
        to the history each time it is updated. If the state is mutable, it will be
        changed in the history as well. This will lead to unexpected behavior.

        IMPORTANT: Ensure that the method returns the state object if it is changed. The
        results of this method are used as input for the next method.

        Args:
            state (State): Current state

        Returns:
            State: Updated state
        """
        pass

    @abstractmethod
    def get_task(self, state: State) -> Return:
        """Called after the interpretation process ends

        Args:
            state (State): Current state

        Returns:
            Task: Task to run
        """
        pass
