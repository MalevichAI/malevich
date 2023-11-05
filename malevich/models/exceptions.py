from typing import Any

from ..interpreter.abstract import Interpreter


class InterpretationError(Exception):
    def __init__(
        self, message: str, interpreter: Interpreter, state: Any = None  # noqa: ANN401
    ) -> None:
        super().__init__(message)
        self.interpreter = interpreter
        self.state = state or interpreter.state
