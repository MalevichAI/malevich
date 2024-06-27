from abc import ABC, abstractmethod
from typing import Any, Generic, Optional, TypeVar

from pydantic import BaseModel

T = TypeVar("T")
Key = TypeVar("Key")
# Data = TypeVar("Output")

class BaseInjectable(BaseModel, Generic[T, Key], ABC):
    @abstractmethod
    def get_inject_key(self) -> Key:
        pass

    @abstractmethod
    def get_inject_data(self) -> T:
        pass

class CoreInjectable(BaseInjectable[str, str]):

    collection_id: str
    alias: str
    uploaded_id: Optional[str] = None
    node: Any = None

    def get_inject_data(self) -> str:
        return self.collection_id

    def get_inject_key(self) -> str:
        return self.alias

    def get_uploaded_id(self) -> Optional[str]:
        return self.uploaded_id


class SpaceInjectable(BaseInjectable[str, str]):
    in_flow_id: str | None = None
    snapshot_flow_id: str | None = None
    alias: str | None = None
    reverse_id: str | None = None


    def get_inject_data(self) -> str:
        return self.snapshot_flow_id

    def get_inject_key(self) -> str:
        return self.alias
