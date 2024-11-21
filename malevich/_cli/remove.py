from typing import Annotated

import rich
import typer
import typer.core

from malevich._utility import FlowStub, PackageManager
from malevich.manifest import ManifestManager

from .prefs import prefs as prefs


def remove(
    component_names: Annotated[list[str], typer.Argument(...)],
) -> None:
    try:
        manf = ManifestManager()
        removed = []
        for component in manf.query('flows'):
           if component in component_names:
                FlowStub.remove_stub(component)
                manf.remove('flows', component)
                removed.append(component)

        for component in component_names:
            if component in removed:
                continue
            manf.remove(
                'dependencies',
                component,
            )
            PackageManager().remove_stub(component)

        rich.print(
            f"[green]Component(s) [b]{', '.join(component_names)}[/b] removed[/green]"
        )
        rich.print(f"Bye, bye [b]{component}[/b]")
    except Exception as e:
        rich.print(
            f"[red]Failed to remove component [b]{component}[/b][/red]")
        rich.print(e)
        return
