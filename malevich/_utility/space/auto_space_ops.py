from malevich_space.ops import SpaceOps

from .space import resolve_setup


def get_auto_ops() -> SpaceOps:
    from malevich.manifest import ManifestManager

    return SpaceOps(resolve_setup(
        ManifestManager().query('space', resolve_secrets=True)
    ))
