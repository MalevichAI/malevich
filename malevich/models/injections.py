from abc import ABC, abstractmethod
from typing import Generic, TypeVar

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
