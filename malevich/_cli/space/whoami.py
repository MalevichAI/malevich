import rich
from malevich_space.ops import SpaceOps
from malevich_space.schema import SpaceSetup
from pydantic import ValidationError

from malevich.manifest import ManifestManager


def get_user_on_space() -> None:
    manf = ManifestManager()
    if (setup := manf.query('space', resolve_secrets=True)) is None:
        rich.print("You are not connect to Malevich Space. Run `malevich space login`")

    if not isinstance(setup, dict):
        rich.print(
            "Manifest is invalid. `space:` should be a valid mapping, "
            f"but it is {type(setup)}"
        )
        return


    try:
        setup = SpaceSetup(**setup)
    except ValidationError as ve:
        rich.print(
            "Manifest is invalid. `space:` does not comply to the schema, "
            + ', '.join([e.msg for e in ve.errors()])
        )
        return
    try:
        SpaceOps(setup)
    except Exception as e:
        rich.print(f"Could not login: {type(e)}({e}). Run `malevich space login`")
        return
    rich.print(f"You are [yellow]{setup.username}[/yellow] at "
               f"{setup.api_url.replace('api.', 'space.', 1)}")
    rich.print(
        f"Active manifest is [i][bright_black] { manf.path } [/i][/bright_black]"
    )
