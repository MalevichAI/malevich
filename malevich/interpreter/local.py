import json
import re
from pathlib import Path

from malevich_coretools import Condition, JsonImage, Processor, Result

from malevich._autoflow.tracer import traced
from malevich._utility import unique
from malevich._utility.logging import LogLevel, cout
from malevich._utility.registry import Registry
from malevich.interpreter.abstract import Interpreter
from malevich.interpreter.core import CoreInterpreter
from malevich.models import BaseNode
from malevich.models.actions import Action
from malevich.models.nodes.asset import AssetNode
from malevich.models.nodes.collection import CollectionNode
from malevich.models.nodes.document import DocumentNode
from malevich.models.nodes.operation import OperationNode
from malevich.models.preferences import VerbosityLevel
from malevich.models.state.local import LocalInterpreterState
from malevich.models.task.interpreted.local import LocalTask

_levels = [
    LogLevel.Info,
    LogLevel.Warning,
    LogLevel.Error,
    LogLevel.Debug
]

_actions = [
    Action.Interpretation,
    Action.Preparation,
    Action.Run,
    Action.Results
]

def _log(
    message: str,
    level: int = 0,
    action: int = 0,
    step: bool = False,
    *args,
) -> None:
    cout(
        _actions[action],
        message,
        verbosity=VerbosityLevel.AllSteps if step else VerbosityLevel.OnlyStatus,
        level=_levels[level],
        *args,
    )

registry = Registry()

class LocalInterpreter(CoreInterpreter):
    def __init__(self) -> None:
        super(CoreInterpreter, self).__init__(LocalInterpreterState())
        self._state = LocalInterpreterState()
        self.update_state()

    def before_interpret(self, state: LocalInterpreterState) -> LocalInterpreterState:
        _log("Interpretation in local mode", 0, 0, True)
        return state

    def create_node(
        self,
        state: LocalInterpreterState,
        tracer: traced[BaseNode]
    ) -> LocalInterpreterState:
        match tracer.owner:
            case OperationNode():
                if tracer.owner.alias is None:
                    tracer.owner.alias = unique(tracer.owner.processor_id)
                
                state.operation_nodes[tracer.owner.alias] = tracer.owner

                extra = registry.get(tracer.owner.operation_id)
                if extra is not None:
                    if 'package_path' not in extra:
                        raise ValueError(
                            f'Processor {tracer.owner.processor_id} has '
                            'installed with installer different from "local". '
                            'Using local mode is only available for local '
                            'installations.'
                        )

                    package_path = Path(extra['package_path'])

                    if not package_path.exists():
                        raise FileNotFoundError(
                            f'Processor {tracer.owner.processor_id} has '
                            f'been installed from path {package_path}, which '
                            'does not exist.'
                        )

                else:
                    raise ValueError(
                        f'Failed to find processor {tracer.owner.processor_id} '
                        'in the registry.'
                    )

                state.import_paths.add(str(package_path.absolute()))

                if tracer.owner.is_condition:
                    state.conditions[tracer.owner.alias] = Condition(
                        arguments={},
                        cfg=json.dumps(tracer.owner.config),
                        image=JsonImage(ref='', user='', token=''),
                        conditionId=tracer.owner.processor_id
                    )
                else:
                    state.processors[tracer.owner.alias] = Processor(
                        arguments={},
                        cfg=json.dumps(tracer.owner.config),
                        image=JsonImage(ref=''),
                        processorId=tracer.owner.processor_id
                    )

                state.results[tracer.owner.alias] = [Result(name=tracer.owner.alias)]

            case CollectionNode():
                if tracer.owner.alias is None:
                    tracer.owner.alias = unique(tracer.owner.collection.collection_id)
                state.collection_nodes[tracer.owner.alias] = tracer.owner
            case AssetNode():
                tracer.owner.alias = unique(tracer.owner.name)
                state.asset_nodes[tracer.owner.alias] = tracer.owner
            case DocumentNode():
                if tracer.owner.alias is None:
                    tracer.owner.alias = unique(tracer.owner.reverse_id)
                state.document_nodes[tracer.owner.alias] = tracer.owner
            case _:
                pass

        _log(f"Node: {tracer.owner.uuid}, {tracer.owner.short_info()}", -1, 0, True)
        return state

    def get_task(self, state: LocalInterpreterState) -> LocalTask:
        return LocalTask(state)

    def attach(
        self,
        unique_task_hash: str,
        only_fetch: bool = False,
    ) -> LocalTask:
        raise NotImplementedError("Local interpreter does not support attach method")
