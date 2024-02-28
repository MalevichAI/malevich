import os
import sys

MALEVICH_HOME = os.getenv("MALEVICH_HOME", os.path.expanduser('~/.malevich/'))
MALEVICH_CACHE =  os.path.expanduser(os.getenv("MALEVICH_CACHE", '~/.malevich/cache'))

class Paths:
    @staticmethod
    def home(*path, create: bool = False, create_dir: bool = False) -> str:
        if create:
            os.makedirs(os.path.join(MALEVICH_HOME, *path), exist_ok=True)
        if create_dir:
            os.makedirs(
                os.path.dirname(os.path.join(MALEVICH_HOME, *path)),
                exist_ok=True
            )
        return os.path.join(MALEVICH_HOME, *path)

    @staticmethod
    def cache(*path) -> str:
        return os.path.join(MALEVICH_CACHE, *path)

    @staticmethod
    def pwd(*path) -> str:
        return os.path.join(os.getcwd(), *path)

    @staticmethod
    def module(*path) -> str:
        return os.path.join(os.path.dirname(sys.modules['malevich'].__file__), *path)
