from typing import Any, Optional

from pydantic import BaseModel

from .injections import SpaceInjectable


class Dependency(BaseModel):
    package_id: str
    version: Optional[str] = None
    installer: str
    options: Optional[Any] = None

    def simple(self) -> dict[str, Any]:
        return {
            **self.options.model_dump()
        }

    def compatible_with(
        self,
        other: 'Dependency',
        compatability_strategy: 'CompatabilityStrategy' = None
    ) -> bool:
        pass

    def checksum(self) -> str:
        pass


class Integration(BaseModel):
    version: Optional[str] = None
    branch: Optional[str] = None
    deployment: Optional[str] = None
    injectables: list[SpaceInjectable] = []
