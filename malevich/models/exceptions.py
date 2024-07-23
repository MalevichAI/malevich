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
class ForbiddenSyntaxError(SyntaxError): ...

class NewVariableInBranchError(SyntaxError):
    message = (
        "Malevich @flow definition does not support branches "
        "that introduces a new variable. Declare the variable "
        "in the upper-level of the function "
        "\n@flow"
        "\ndef {function_name}(...):"
        "\n\t{name} = None"
        "\n\t# ..."
        "\n"
        "\n or consider using existing variables in the branch. "
        "Variable: {name}"
    )
    
    def finalize(
        self,
        filename: str,
        function_name: str | None = None,
        var_name: str | None = None
    ):
        self.args = (
            self.message.format(
                function_name=function_name,
                name=var_name or self.var_name
            ),
            (filename, *self.details[1:])
        )
        super().__init__(*self.args)
        return self
    
    def __init__(
        self,
        var_name: str,
        function_name: str | None,
        details,
    ):
        self.var_name = var_name
        self.details = details
        if function_name:
            self.args[0] = self.message.format(
                function_name=function_name,
                name=var_name
            )
