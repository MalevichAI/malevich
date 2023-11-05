import os
from typing import IO

from ..singleton import SingletonMeta


class CacheManager(metaclass=SingletonMeta):
    def __init__(self) -> None:
        self._fs = os.path.expanduser('~/.malevich/cache')

    def get_file(self, path: str, mode: str) -> IO | None:
        return open(self.make_path_in_cache(path), mode)

    def make_path_in_cache(self, base_path: str) -> str:
        os.makedirs(
            os.path.dirname(os.path.join(self._fs, base_path),),
            exist_ok=True
        )
        return os.path.join(self._fs, base_path)
