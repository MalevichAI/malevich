import hashlib
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel

from malevich.models.dependency import Dependency


class LocalOptions(BaseModel):
    import_path: Path
    dependencies: Optional[list[str]] = None


class LocalDependency(Dependency):
    installer: str = 'local'
    options: Optional[LocalOptions] = None

    def simple(self) -> dict[str, Any]:
        simpler = {**self.options.model_dump()}
        return {k: v for k, v in simpler.items() if v is not None}

    def checksum(self) -> str:
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()

    def compatible_with(
        self,
        other: 'Dependency',
        compatability_strategy,
    ) -> bool:
        return False
