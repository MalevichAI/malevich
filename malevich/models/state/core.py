from copy import deepcopy
from typing import Any

import malevich_coretools as core
from pydantic import BaseModel, ConfigDict, Field, SkipValidation

from ..._core.service.service import CoreService
from .base import BaseCoreState


class CoreParams(BaseModel):
    operation_id: str | None = None
    task_id: str | None = None
    core_host: str | None = None
    core_auth: tuple[str, str] | None = None
    base_config: core.Cfg | None = None
    base_config_id: str | None = None

    def __getitem__(self, key: str) -> Any:  # noqa: ANN401
        return getattr(self, key)

    def __setitem__(self, key: str, value: Any) -> None:  # noqa: ANN401
        setattr(self, key, value)

    def __contains__(self, key: str) -> bool:
        return hasattr(self, str(key))


class CoreInterpreterState(BaseCoreState):
    """State of the CoreInterpreterV2"""
    model_config = ConfigDict(
        arbitrary_types_allowed=True
    )

    service: SkipValidation[CoreService] | None = Field(default=None, exclude=True)
    """CoreService instance"""

    params: CoreParams = CoreParams()
    """Interpreter parameters"""



    def __deepcopy__(self, memo=None) -> "CoreInterpreterState":
        # Deepcopy the state without the service
        return CoreInterpreterState(**deepcopy(self.model_dump()), service=self.service)
