from typing import Optional

from pydantic import BaseModel

from ..dependency import Dependency


class SpaceDependencyOptions(BaseModel):
    reverse_id: str
    branch: Optional[str] = None
    version: Optional[str] = None
    image_ref: Optional[str] = None
    image_auth_user: Optional[str] = None
    image_auth_pass: Optional[str] = None


class SpaceDependency(Dependency):
    options: SpaceDependencyOptions
