import logging
# Autoflow Engine
from ._autoflow import tracer as gn
from ._autoflow import tree as tree
from ._autoflow.function import *
from ._autoflow.flow import *

# Metascript
from ._meta.collection import collection
from ._meta.flow import flow
from ._meta.config import config

# Manifest
from .manifest import *
from .models.manifest import *

# Expose contants
from .constants import *

# Summaries
from ._utility.summary import *

manifest = ManifestManager()


import malevich_coretools as core
__logger = logging.getLogger("log")
__logger.setLevel(logging.CRITICAL)
core.set_logger(__logger)

from importlib.machinery import SourceFileLoader

globals()['utility'] = SourceFileLoader('utility', '/home/teexone/.malevich/envs/default/utility/__init__.py').load_module()
import importlib.util
import sys
spec = importlib.util.spec_from_file_location("malevich.utility", "/home/teexone/.malevich/envs/default/utility/__init__.py")
utility = importlib.util.module_from_spec(spec)
sys.modules["malevich.utility"] = utility
spec.loader.exec_module(utility)