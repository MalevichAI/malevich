from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar

from .nodes.collection import CollectionNode

T = TypeVar("T")
Key = TypeVar("Key")
# Data = TypeVar("Output")

class BaseInjectable(Generic[T, Key], ABC):
    @abstractmethod
    def get_inject_key(self) -> Key:
        pass

    @abstractmethod
    def get_inject_data(self) -> T:
        pass

class CoreInjectable(BaseInjectable[str, str]):
    def __init__(
        self,
        collection_id: str,
        alias: str,
        uploaded_id: Optional[str] = None,
        node: CollectionNode | None = None,
    ) -> None:
        self.__collection_id = collection_id
        self.__alias = alias
        self.__uploaded_id = uploaded_id
        self.__node = node

    def get_inject_data(self) -> str:
        return self.__collection_id

    def get_inject_key(self) -> str:
        return self.__alias

    def get_uploaded_id(self) -> Optional[str]:
        return self.__uploaded_id

    @property
    def node(self) -> CollectionNode | None:
        return self.__node


class SpaceInjectable(BaseInjectable[str, str]):
    def __init__(
        self,
        in_flow_id: str,
        snapshot_flow_id: str,
        alias: str
    ) -> None:
        self.__in_flow_id = in_flow_id
        self.__alias = alias
        self.__snapshot_flow_id = snapshot_flow_id

    @property
    def alias(self) -> str:
        return self.__alias

    @property
    def in_flow_id(self) -> str:
        return self.__in_flow_id

    @property
    def snapshot_flow_id(self) -> str:
        return self.__snapshot_flow_id

    def __repr__(self) -> str:
        return f"SpaceInjectable(alias={self.__alias}, in_flow_id={self.__in_flow_id})"

    __str__ = __repr__

    def get_inject_data(self) -> str:
        return self.__snapshot_flow_id

    def get_inject_key(self) -> str:
        return self.__alias
