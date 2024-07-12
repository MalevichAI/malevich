
from typing import TypeVar

from malevich._dev.singleton import SingletonMeta

T = TypeVar('T')

class Registry(metaclass=SingletonMeta):
    _registry = {}

    @property
    def registry(self) -> dict:
        return self._registry

    def register(self, key: str, value: object) -> None:
        self._registry[key] = value

    def get(self, key: str, default=None, model: T = None) -> T | object | None:
        _o = self._registry.get(key, default)
        if model and not isinstance(_o, model):
            return model(**self._registry.get(key, default))
        return self._registry.get(key, default)

    def __getitem__(self, key: str) -> object:
        return self._registry[key]

    def merge_keys(self, key: str, value: dict) -> None:
        if key not in self._registry:
            self._registry[key] = {}
        if not isinstance(self._registry[key], dict):
            raise ValueError(f"Key {key} is not a dictionary")

        # Deep merge of dictionaries
        self._registry[key] = self.__merge_dicts(self._registry[key], value)

    def __merge_dicts(self, dict1: dict, dict2: dict) -> dict:
        for key in dict2:
            if key in dict1:
                if isinstance(dict1[key], dict) and isinstance(dict2[key], dict):
                    dict1[key] = self.__merge_dicts(dict1[key], dict2[key])
                elif isinstance(dict1[key], list) and isinstance(dict2[key], list):
                    dict1[key] = self.__merge_lists(dict1[key], dict2[key])
                else:
                    dict1[key] = dict2[key]
            else:
                dict1[key] = dict2[key]
        return dict1

    def __merge_lists(self, list1: list, list2: list) -> list:
        for item in list2:
            if item not in list1:
                list1.append(item)
        return list1
