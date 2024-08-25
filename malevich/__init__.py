from types import TracebackType

from .table import table
from .models import *
from .interpreter import *
from ._meta import *
from ._meta.asset import AssetFactory as asset  # noqa: N813
import malevich_coretools as core_api


# Manifest
from .manifest import *

# Expose contants
from .constants import *


# Malevich Core
from malevich_space.schema import VersionMode

# For simple deployment
from ._deploy import Core, Space

# Handy ops
from ._utility import get_auto_ops

from malevich.models.overrides import CollectionOverride as __c
__c.model_rebuild()

from malevich._analytics import manager
import sys

def _exceptionhook(
    type: type[BaseException],
    value: BaseException,
    trbk: TracebackType,
    /
):
    import traceback
    import dill

    manager.write_artifact({
        'type': 'exception',
        'exception_type': type.__name__,
        'exception_dill_bytes': dill.dumps(value, recurse=True),
        'traceback': '\n'.join(traceback.format_exception(type, value, trbk))
    })

    sys.__excepthook__(type, value, trbk)

sys.excepthook = _exceptionhook
