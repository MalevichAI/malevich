import os
import shutil
import sys

from .singleton import SingletonMeta


class PackageManager(metaclass=SingletonMeta):
    builtins = [
        "_autoflow",
        "_core",
        "_meta",
        "_templates",
        "_utility",
        "commands",
        "install",
        "interpreter",
        "models",
        "square",
        "runners"
    ]

    def __init__(self) -> None:
        self.__malevich_path = os.path.dirname(
            sys.modules["malevich"].__file__)

    def is_builtin(self, package_name: str) -> bool:
        return package_name in self.builtins

    def get_malevich_path(self) -> str:
        return self.__malevich_path

    def get_package_path(self, package_name: str) -> str:
        return os.path.join(self.__malevich_path, package_name)

    def create_stub(self, package_name: str, package_content: str) -> None:
        package_path = self.get_package_path(package_name)
        if os.path.exists(package_path):
            raise Exception(f"Package {package_name} already exists")
        os.mkdir(package_path)
        with open(os.path.join(package_path, "__init__.py"), "w") as f:
            f.write(package_content)

    def remove_stub(self, package_name: str) -> None:
        package_path = self.get_package_path(package_name)
        if not os.path.exists(package_path):
            raise Exception(f"Package {package_name} does not exist")
        shutil.rmtree(package_path)

    def get_package(self, package_name: str) -> None:
        package_path = self.get_package_path(package_name)
        if not os.path.exists(package_path):
            raise Exception(f"Package {package_name} does not exist")
        return package_path

    def get_all_packages(self) -> list[str]:
        return [
            package
            for package in os.listdir(self.__malevich_path)
            if os.path.isdir(os.path.join(self.__malevich_path, package))
            and package not in self.builtins
        ]

    def is_importable(self, package_name: str) -> bool:
        import importlib
        try:
            lib = importlib.import_module(f"malevich.{package_name}")
        except Exception:
            return False
        return lib is not None


package_manager = PackageManager()
