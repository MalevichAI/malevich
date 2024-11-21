from typing import Any, Optional

from pydantic import BaseModel


class CoreRegistryEntry(BaseModel):
    image_ref: tuple[str, ...] | str
    processor_id: str
    image_auth_user: Optional[tuple[str, ...] | str] = None
    image_auth_pass: Optional[tuple[str, ...] | str] = None

    def __getattribute__(self, __name: str) -> str:
        from malevich.manifest import manf

        attr_ = super().__getattribute__(__name)
        if __name in ['image_auth_user', 'image_auth_pass', 'image_ref']:
            if isinstance(attr_, tuple):
                return manf.query(*attr_, resolve_secrets=True)
        return attr_

    def __getitem__(self, key: str) -> Any:  # noqa: ANN401
        # Backward compatability with dictionary
        # bypassing `self.__getattribute__`
        # to not query for manifested values
        # in case of access like `extra["image_ref"]`
        return super().__getattribute__(key)
