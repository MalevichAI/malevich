from typing import Any, Optional

from malevich_space.schema import SpaceSetup
from pydantic import BaseModel

from .preferences import UserPreferences


class Dependency(BaseModel):
    package_id: str
    version: str
    installer: str
    options: Any

    def simple(self) -> dict[str, Any]:
        return {
            **self.options.model_dump()
        }


class Manifest(BaseModel):
    project_id: Optional[str] = None
    version: Optional[str] = None
    space: Optional[SpaceSetup] = None
    dependencies: list[dict[str, Dependency]] = []
    preferences: UserPreferences = UserPreferences()


class ManifestUpdateEntry(BaseModel):
    package_id: str
    version: str
    installer: str
    options: dict[str, Any]


class Secret(BaseModel):
    secret_key: str
    secret_value: str
    salt: Optional[str] = None
    external: Optional[bool] = False


class Secrets(BaseModel):
    secrets: dict[str, Secret] = {}
