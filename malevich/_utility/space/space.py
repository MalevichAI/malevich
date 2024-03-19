from malevich_space.schema import SpaceSetup

from ...manifest import ManifestManager

manf = ManifestManager()


def resolve_setup(space: dict) -> SpaceSetup:
    space = SpaceSetup(**space)

    if manf.is_secret(space.password):
        space.password = manf.query_secret(space.password, only_value=True)

    return space
