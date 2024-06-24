from typing import Annotated

import rich
import typer
import typer.core

from malevich._utility.package import PackageManager

from .._cli.prefs import prefs as prefs
from ..install.flow import FlowInstaller
from ..manifest import ManifestManager
from ..models.dependency import Integration


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

def delete_flow(
    reverse_id: str=typer.Argument(
        ...,
        show_default=False,
        help="Space Flow Reverse ID"
    ),
    version: str=typer.Option(
        None,
        '--version',
        '-v',
        show_default=False,
        help="Version to delete."
    ),
    delete_all: bool=typer.Option(
        False,
        '--all-versions',
        '-a',
        show_default=False,
        help="Delete all versions of the flow."
    )
):
    if version is None and not delete_all:
        rich.print(
            "Either [violet]--version[/violet] or [violet]--all-versions[/violet] "
            "should be provided."
        )
        exit(1)
    installer = FlowInstaller()
    if delete_all:
        try:
            installer.remove(reverse_id)
        except Exception as e:
            rich.print(f"[red]{e}[/red]")
            exit(1)
    else:
        try:
            installer.remove(reverse_id, Integration(version=version))
        except Exception as e:
            rich.print(f"Failed to remove {reverse_id}: {e}")

    rich.print(
        f"{'All versions' if delete_all else 'version ' + version} "
        f"of [yellow]{reverse_id}[/yellow] was [green]successfully[/green] "
        f"deleted."
    )
