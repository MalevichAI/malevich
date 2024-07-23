import hashlib
from typing import Optional

from pydantic import BaseModel

from malevich.models.dependency import Dependency

from .compat import CompatabilityStrategy, compare_images


class SpaceDependencyOptions(BaseModel):
    reverse_id: str
    branch: Optional[str] = None
    version: Optional[str] = None
    image_ref: Optional[str] = None
    image_auth_user: Optional[str] = None
    image_auth_pass: Optional[str] = None


class SpaceDependency(Dependency):
    installer: str = 'space'
    options: SpaceDependencyOptions

    def checksum(self) -> str:
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()

    def compatible_with(
        self, other: Dependency, compatability_strategy: CompatabilityStrategy
    ) -> bool:
        if isinstance(other, SpaceDependency):
            compat = self.options.reverse_id == other.options.reverse_id
            if self.options.branch is not None:
                compat &= self.options.branch == other.options.branch
            if self.options.version is not None:
                compat &= self.options.version == other.options.version
            if self.options.image_ref is not None:
                compat &= compare_images(
                    self.options.image_ref,
                    other.options.image_ref,
                    compatability_strategy
                )
            return compat
        return False
