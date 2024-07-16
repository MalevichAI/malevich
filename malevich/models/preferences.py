from enum import Enum

from pydantic import BaseModel

from .actions import Action


class VerbosityLevel(Enum):
    Quiet = 0
    OnlyStatus = 1
    AllSteps = 2


class LogFormat(Enum):
    Plain = "PLAIN"
    Rich = "RICH"


class UserPreferences(BaseModel):
    verbosity: dict[str, int] = {
        Action.Interpretation.value: VerbosityLevel.Quiet.value,
        Action.Preparation.value: VerbosityLevel.OnlyStatus.value,
        Action.Run.value: VerbosityLevel.OnlyStatus.value,
        Action.Stop.value: VerbosityLevel.Quiet.value,
        Action.Installation.value: VerbosityLevel.Quiet.value,
        Action.Removal.value: VerbosityLevel.Quiet.value,
        Action.Results.value: VerbosityLevel.OnlyStatus.value,
        Action.Attachment.value: VerbosityLevel.OnlyStatus.value,
    }
    log_format: LogFormat = LogFormat.Rich
    log_level: str = "INFO"
