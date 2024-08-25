import sys
from functools import cache
from pathlib import Path


@cache
def get_malevich_version():
    module_path = Path(sys.modules['malevich'].__file__)
    malevich_version = open(module_path.parent.parent / 'VERSION').read().strip()
    return malevich_version