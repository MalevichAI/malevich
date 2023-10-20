from typing import Optional

from pydantic import BaseModel

from ...constants import DEFAULT_CORE_HOST
from ...models.manifest import Dependency


class ImageOptions(BaseModel):
    image_ref: str
    image_auth_user: Optional[str]
    image_auth_pass: Optional[str]
    core_host: Optional[str] = DEFAULT_CORE_HOST
    core_auth_user: Optional[str]
    core_auth_token: Optional[str]
    checksum: str
    operations: dict[str, str] = None


class ImageDependency(Dependency):
    options: ImageOptions
