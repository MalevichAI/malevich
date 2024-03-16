from malevich_space.constants import *  # noqa: F403

from .models.platform import Platform
from .path import MALEVICH_CACHE, MALEVICH_HOME  # noqa: F401

DEFAULT_CORE_HOST = "https://core.malevich.ai/"

APP_HELP = """

"""

USE_HELP = """
Install apps to get access to the processors they provide
"""

USE_IMAGE_HELP = """
Install apps using published Docker images. The information about
the functionality provided by them will be pulled and parsed accordingly.
"""

USE_SPACE_HELP = """
Install apps using published Docker images. The information about
the functionality provided by them will be pulled and parsed accordingly.
"""

IMAGE_BASE = "public.ecr.aws/o1z1g3t0/{app}:latest"

CORE_INTERPRETER_IN_APP_INFO_KEY = '__core__'

CorePlatform = Platform.CORE
SpacePlatofrm = Platform.SPACE


reserved_config_fields = [
    ('alias', 'str'),
]

