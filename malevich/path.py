import os
import sys
from pathlib import Path as _Path

MALEVICH_HOME = os.getenv("MALEVICH_HOME", os.path.expanduser('~/.malevich/'))
MALEVICH_CACHE =  os.path.expanduser(os.getenv("MALEVICH_CACHE", '~/.malevich/cache'))
DB_PATH = '.db'

class Paths:
    @staticmethod
    def home(*path, create: bool = False, create_dir: bool = False) -> str:
        path = list(path)
        if create:
            os.makedirs(os.path.join(MALEVICH_HOME, *path[:-1]), exist_ok=True)
            _Path(os.path.join(MALEVICH_HOME, *path)).touch()
        if create_dir:
            os.makedirs(
                os.path.join(MALEVICH_HOME, *path),
                exist_ok=True
            )
        return os.path.join(MALEVICH_HOME, *path)

    @staticmethod
    def cache(*path) -> str:
        return os.path.join(MALEVICH_CACHE, *path)

    @staticmethod
    def pwd(*path, create: bool = False) -> str:
        if create:
            p = _Path(os.path.join(os.getcwd(), *path))
            p.touch()
            return str(p)
        return os.path.join(os.getcwd(), *path)

    @staticmethod
    def module(*path) -> str:
        return os.path.join(os.path.dirname(sys.modules['malevich'].__file__), *path)

    @staticmethod
    def db() -> str:
        return Paths.home(DB_PATH)

    @staticmethod
    def templates(*args: os.PathLike) -> str:
        return Paths.module('_templates', *args)

    @staticmethod
    def local(*args: os.PathLike, create: bool = False, create_dir: bool = False) -> str:
        return Paths.home('.local', *args, create=create, create_dir=create_dir)

    @staticmethod
    def local_results(
        operation_id: str,
        run_id: str,
        index: int,
        create: bool = False,
        create_dir: bool = False,
    ) -> str | None:
        for path in os.listdir(
            Paths.local(
                'results',
                operation_id,
                run_id,
                create=create,
                create_dir=create_dir
            )
        ):
            if path.startswith(str(index)):
                return Paths.local(
                    'results',
                    operation_id,
                    run_id,
                    path,
                    create=create,
                    create_dir=create_dir
                )
        return None

