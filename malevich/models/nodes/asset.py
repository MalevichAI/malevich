import hashlib
from functools import cache
from typing import Optional

from .base import BaseNode


class AssetNode(BaseNode):
    real_path: Optional[str | list[str]] = None
    core_path: str
    is_composite: Optional[bool] = False
    persistent: Optional[bool] = False

    @cache
    def magic(self) -> str:
        if self.persistent:
            return hashlib.sha256(self.core_path).hexdigest()
        else:
            bytes_ = b""
            if self.is_composite:
                for file in self.real_path:
                    with open(file, "rb") as f:
                        bytes_ += f.read()
                else:
                    with open(self.real_path, "rb") as f:
                        bytes_ += f.read()
            bytes_ += self.core_path
            return hashlib.sha256(bytes_).hexdigest()

    def get_core_path(self) -> str:
        return '$'+self.core_path

    def __hash__(self) -> int:
        return super().__hash__()
