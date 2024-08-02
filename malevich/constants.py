import os

from malevich_space.constants import *  # noqa: F403

from .path import MALEVICH_CACHE, MALEVICH_HOME, Paths  # noqa: F401

DEFAULT_CORE_HOST = "https://core.malevich.ai/"

APP_HELP = """
Malevich is a co
"""

USE_HELP = """
Install apps to get access to the processors they provide
"""

USE_IMAGE_HELP = """
Install apps using published Docker images. The information about
the functionality provided by them will be pulled and parsed accordingly.
"""

USE_SPACE_HELP = """
Install components from Malevich Space repository.
"""

IMAGE_BASE = "public.ecr.aws/o1z1g3t0/{app}:latest"

CORE_INTERPRETER_IN_APP_INFO_KEY = '__core__'

reserved_config_fields = [
    ('alias', 'str'),
]

TEST_DIR =  os.path.expanduser(
    os.getenv("MALEVICH_TEST_DIR", Paths.home('testing', create_dir=True))
)

