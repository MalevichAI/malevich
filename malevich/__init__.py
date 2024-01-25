import malevich_coretools as core
import logging
# Autoflow Engine
from ._autoflow import tracer as gn
from ._autoflow import tree as tree
from ._autoflow.function import autotrace
from ._autoflow.flow import *

# Metascript
from ._meta.collection import collection
from ._meta.flow import flow
from ._meta.config import config
from ._meta.run import run
# HACK: For users to see the purpose
# of `asset`, the class is imported
from ._meta.asset import AssetFactory as asset  # noqa: N813

# Manifest
from .manifest import *
from .models.manifest import *

# Expose contants
from .constants import *

# # Summaries
# from ._utility.summary import *

# Interpreters
from .interpreter.core import CoreInterpreter
from .interpreter.space import SpaceInterpreter


manifest = ManifestManager()

__logger = logging.getLogger("log")
__logger.setLevel(logging.CRITICAL)
core.set_logger(__logger)
