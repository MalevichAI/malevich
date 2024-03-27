import os
import shutil
import tempfile

import yaml
from malevich_space.constants import ACTIVE_SETUP_PATH
from malevich_space.ops.env import get_file_path

from ...manifest import ManifestManager


class __ManifestAsEnv:  # noqa: N801
    def __init__(self) -> None:
        self.space = None
        self.tmp = None
        self.__path = get_file_path(ACTIVE_SETUP_PATH)

    def __enter__(self) -> None:
        manf = ManifestManager()
        space = manf.query('space', resolve_secrets=True)
        self.space = {'space': space}
        self.tmp = tempfile.NamedTemporaryFile('w+')
        if os.path.exists(self.__path):
            shutil.copyfileobj(
                open(self.__path),
                self.tmp
            )

        self.tmp.seek(0)
        with open(self.__path, 'w') as f:
            yaml.dump(self.space, f)

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        old_space = yaml.load(self.tmp, Loader=yaml.FullLoader)
        self.tmp.close()
        with open(self.__path, 'w') as f:
            yaml.dump(old_space, f)

        if exc_type:
            return False
        return True

manifest_as_env = __ManifestAsEnv()
