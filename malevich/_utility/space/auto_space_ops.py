from malevich_space.ops import SpaceOps

from ...manifest import ManifestManager
from .space import resolve_setup


def get_auto_ops() -> SpaceOps:
    return SpaceOps(resolve_setup(
        ManifestManager().query('space', resolve_secrets=True)
    ))
