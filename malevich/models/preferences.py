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
    verbosity: dict[Action, VerbosityLevel] = {
        Action.Interpretation: VerbosityLevel.Quiet,
        Action.Preparation: VerbosityLevel.OnlyStatus,
        Action.Run: VerbosityLevel.OnlyStatus,
        Action.Stop: VerbosityLevel.Quiet,
        Action.Installation: VerbosityLevel.Quiet,
        Action.Removal: VerbosityLevel.Quiet,
        Action.Results: VerbosityLevel.OnlyStatus,
    }
    log_format: LogFormat = LogFormat.Rich
    log_level: str = "INFO"
