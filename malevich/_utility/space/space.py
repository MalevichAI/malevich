from malevich_space.schema import SpaceSetup


def resolve_setup(space: dict) -> SpaceSetup:
    from malevich.manifest import manf
    space = SpaceSetup(**space)

    if manf.is_secret(space.password):
        space.password = manf.query_secret(space.password, only_value=True)

    return space
