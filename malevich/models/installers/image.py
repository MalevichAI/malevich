from typing import Optional

from pydantic import BaseModel

from malevich.models.manifest import Dependency


class ImageOptions(BaseModel):
    image_ref: str
    image_auth: tuple[str, str]
    core_host: str
    core_auth: Optional[str]
    checksum: str


class ImageDependency(Dependency):
    options: ImageOptions
