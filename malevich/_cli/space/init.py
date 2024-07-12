from typing import Annotated

import pydantic_yaml as pdyml
import rich
import typer
from malevich_space.schema import Setup
from pydantic import ValidationError

from malevich.manifest import ManifestManager


def init(path_to_setup: Annotated[str, typer.Argument(...)]) -> None:
    try:
        setup = pdyml.parse_yaml_file_as(Setup, path_to_setup)
    except ValidationError as err:
        rich.print(
            f"[b red]Setup file [white]{path_to_setup}[/white] is not a correct configuration[/b red]\n"  # noqa: E501
        )
        for _err in err.errors():
            rich.print(f"\t- {_err['msg']} at [pink]{_err['loc'][0]}[pink]")
        rich.print("\n[red]Could not initialize Space installer[/red]")
    else:
        manf = ManifestManager()

        setup.space.password = manf.put_secret(
            "space_password", setup.space.password)
        ManifestManager().put("space", value=setup.space)

        rich.print(
            "\nMalevich Space configuration [green]successfully[/green] added to the manifest\n"  # noqa: E501
        )
