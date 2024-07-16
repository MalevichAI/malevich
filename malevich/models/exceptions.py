from typing import Any


class InterpretationError(Exception):
    def __init__(
        self, message: str, interpreter: 'Interpreter' = None, state: Any = None  # noqa: ANN401
    ) -> None:
        super().__init__(message)
        if interpreter:
            self.interpreter = interpreter
            self.state = state or interpreter.state


class NoPipelineFoundError(Exception): ...
class NoTaskToConnectError(Exception): ...
