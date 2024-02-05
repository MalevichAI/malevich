from typing import Any

from pydantic import BaseModel


class Dependency(BaseModel):
    package_id: str
    version: str
    installer: str
    options: Any

    def simple(self) -> dict[str, Any]:
        return {
            **self.options.model_dump()
        }
