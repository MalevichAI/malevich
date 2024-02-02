from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar

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
    ) -> None:
        self.__collection_id = collection_id
        self.__alias = alias
        self.__uploaded_id = uploaded_id

    def get_inject_data(self) -> str:
        return self.__collection_id

    def get_inject_key(self) -> str:
        return self.__alias

    def get_uploaded_id(self) -> Optional[str]:
        return self.__uploaded_id
