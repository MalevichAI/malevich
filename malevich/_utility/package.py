import hashlib
import os
import shutil
import sys

from .._dev.singleton import SingletonMeta


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
            sys.modules["malevich"].__file__
        )

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

    def get_package(self, package_name: str) -> str:
        package_path = self.get_package_path(package_name)
        if not self.is_package(package_path):
            raise Exception(f"Package {package_name} does not exist")
        return package_path

    def is_package(self, package_fol_path: str) -> bool:
        is_package = os.path.exists(package_fol_path)
        is_package &= os.path.isdir(package_fol_path)
        init_py = os.path.join(package_fol_path, '__init__.py')
        is_package &= os.path.exists(init_py)
        is_package &= os.path.isfile(init_py)
        is_old_package = False
        is_new_package = False

        F_py = os.path.join(package_fol_path, 'F.py')  # noqa: N806
        scheme_py = os.path.join(package_fol_path, 'scheme.py')
        if os.path.exists(F_py) and os.path.exists(scheme_py):
            is_new_package = True

        if is_package:
            is_old_package = is_package
            with open(init_py) as f:
                str_ = ""
                for line in f.readlines():
                    if line.startswith('__Metascript_checksum__'):
                        exec(line)
                        checksum = locals().get('__Metascript_checksum__')
                        n_checksum =  hashlib.sha256(str_[:-1].encode()).hexdigest()
                        is_old_package &= checksum == n_checksum
                        break
                    else:
                        str_ += line
                else:
                    is_old_package = False
        return is_new_package or is_old_package

    def get_all_packages(self) -> list[str]:
        return [
            package
            for package in os.listdir(self.__malevich_path)
            if self.is_package(self.get_package_path(package))
        ]

    def is_importable(self, package_name: str) -> bool:
        import importlib
        try:
            lib = importlib.import_module(f"malevich.{package_name}")
        except Exception:
            return False
        return lib is not None


package_manager = PackageManager()
