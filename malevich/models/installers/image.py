from typing import Any, Optional

from pydantic import BaseModel

from ...constants import DEFAULT_CORE_HOST
from ...models.manifest import Dependency


class ImageOptions(BaseModel):
    image_ref: str = None
    image_auth_user: Optional[str] = None
    image_auth_pass: Optional[str] = None
    core_host: Optional[str] = DEFAULT_CORE_HOST
    core_auth_user: Optional[str] = None
    core_auth_token: Optional[str] = None
    checksum: str = None
    operations: Optional[dict[str, str]] = None


class ImageDependency(Dependency):
    options: ImageOptions

    def simple(self) -> dict[str, Any]:
        simpler = {**self.options.model_dump()}
        simpler.pop("checksum", None)
        simpler.pop("operations", None)
        return {k: v for k, v in simpler.items() if v is not None}
