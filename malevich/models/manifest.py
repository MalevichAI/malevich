from typing import Any

from pydantic import BaseModel


class Dependency(BaseModel):
    package_id: str
    version: str
    installer: str
    options: dict[str, Any]


class Manifest(BaseModel):
    project_id : str
    version: str
    dependencies: dict[str, Dependency]


class ManifestUpdateEntry(BaseModel):
    package_id: str
    version: str
    installer: str
    options: dict[str, Any]




