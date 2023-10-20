from .._autoflow.tree import ExecutionTree
from ..constants import DEFAULT_CORE_HOST
from ..models.verdict import CanInterpretVerdict
from .abstract import Interpreter
from .core import CoreInterpreter

INTERPRETERS = [
    CoreInterpreter(
        core_host=DEFAULT_CORE_HOST,
        core_auth=None,
    )
]

def auto(tree: ExecutionTree) -> tuple[Interpreter, list[CanInterpretVerdict]]:
    verdicts = []
    interpreter = None
    for _interpreter in INTERPRETERS:
        verdict =_interpreter.can_interpret(tree)
        verdicts.append(verdict)
        if verdict.verdict:
            interpreter = _interpreter
    return interpreter, verdicts
