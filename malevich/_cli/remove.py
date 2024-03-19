from typing import Annotated

import rich
import typer
import typer.core

from malevich._utility.package import PackageManager

from .._cli.prefs import prefs as prefs
from ..manifest import ManifestManager


def remove(
    package_names: Annotated[list[str], typer.Argument(...)],
) -> None:
    try:
        for package_name in package_names:
            manf = ManifestManager()
            manf.remove(
                'dependencies',
                package_name,
            )
            PackageManager().remove_stub(package_name)

        rich.print(
            f"[green]Package(s) [b]{', '.join(package_names)}[/b] removed[/green]"
        )
        rich.print(f"Bye, bye [b]{package_name}[/b]")
    except Exception as e:
        rich.print(
            f"[red]Failed to remove package [b]{package_name}[/b][/red]")
        rich.print(e)
        return
