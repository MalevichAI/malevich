from abc import ABC, abstractmethod


class Installer(ABC):
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def install(self, *args, **kwargs) -> None: # noqa: ANN003, ANN002, ANN204
        pass


    @staticmethod
    def get_package_path() -> str:
        import importlib.util
        import os

        return os.path.dirname(importlib.util.find_spec('malevich').origin)

