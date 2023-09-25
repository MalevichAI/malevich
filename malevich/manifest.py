import os
from typing import Any, Iterable

import yaml

from malevich._utility.singleton import SingletonMeta


class ManifestManager(metaclass=SingletonMeta):
    def __init__(self) -> None:
        self.__path = os.path.join(
            os.getcwd(),
            'malevich.yaml'
        )
        if not os.path.exists(self.__path):
            with open(self.__path, 'w') as _file:
                _file.write('')
        with open(self.__path) as _file:
            self.__manifest = yaml.full_load(_file) or {}

    def query(self, *query: Iterable[str]) -> Any:   # noqa: ANN401
        i = self.__manifest
        return [i := i[k] for k in query][-1]

    def put(self, *path: Iterable[str], value: Any) -> Any:  # noqa: ANN401
        i = self.__manifest
        for k in path[:-1]:
            if k not in i:
                i[k] = {}
            i = i[k]
        i[path[-1]] = value
        with open(self.__path, 'w') as _file:
            yaml.dump(self.__manifest, _file)

        return self.query(*path)
