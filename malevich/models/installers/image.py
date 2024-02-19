import hashlib
from typing import Any, Optional

from pydantic import BaseModel

from ...constants import DEFAULT_CORE_HOST
from ..dependency import Dependency
from .space import SpaceDependency


class ImageOptions(BaseModel):
    image_ref: Optional[str] = None
    image_auth_user: Optional[str] = None
    image_auth_pass: Optional[str] = None
    core_host: Optional[str] = DEFAULT_CORE_HOST
    core_auth_user: Optional[str] = None
    core_auth_token: Optional[str] = None


class ImageDependency(Dependency):
    installer: str = 'image'
    options: Optional[ImageOptions] = ImageOptions()

    def simple(self) -> dict[str, Any]:
        simpler = {**self.options.model_dump()}
        return {k: v for k, v in simpler.items() if v is not None}

    def checksum(self) -> str:
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()

    def compatible_with(self, other: Dependency) -> bool:
        if isinstance(other, ImageDependency):
            compat = self.options.image_ref == other.options.image_ref
            if self.options.image_auth_user is not None:
                compat &= self.options.image_auth_user == other.options.image_auth_user
            if self.options.image_auth_pass is not None:
                compat &= self.options.image_auth_pass == other.options.image_auth_pass
            return compat
        else:
            if isinstance(other, SpaceDependency):
                compat = self.options.image_ref == other.options.image_ref
                if self.options.image_auth_user is not None:
                    compat &= \
                        self.options.image_auth_user == other.options.image_auth_user
                if self.options.image_auth_pass is not None:
                    compat &= \
                        self.options.image_auth_pass == other.options.image_auth_pass
                return compat
        return False
