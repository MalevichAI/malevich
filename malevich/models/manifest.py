from typing import Any, Optional

from pydantic import BaseModel


class Dependency(BaseModel):
    package_id: str
    version: str
    installer: str
    options: dict[str, Any]


class Manifest(BaseModel):
    project_id : Optional[str] = None
    version: Optional[str] = None
    dependencies: list[dict[str, Dependency]] = []


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



