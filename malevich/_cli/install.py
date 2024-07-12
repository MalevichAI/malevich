import logging
import re
from typing import Annotated

import rich
import typer
from rich.progress import Progress, SpinnerColumn, TextColumn

from malevich._deploy import Space
from malevich._utility import parse_kv_args
from malevich.install.flow import FlowInstaller
from malevich.models.dependency import Integration

from .prefs import prefs as prefs
from .use import _install_from_image, _install_from_space

logging.getLogger("gql.transport.requests").setLevel(logging.ERROR)


__With_Args_Help = (
    "Arguments to pass to the installer. "
    "Must be in the format [b]key1=value1,key2=value2[/b] "
    '(" can be used to escape spaces)'
    '\n\n\n'
    'For a particular installer, use [i b] malevich use <installer> --help[/i b] to'
    ' see the list of supported arguments'
)

def auto_use(
    package_names: Annotated[list[str], typer.Argument(...)],
    using: Annotated[str, typer.Option(help="Installer to use")] = "space",
    with_args: Annotated[str, typer.Option(help=__With_Args_Help)] = "",
) -> None:
    rich.print(
        "\n\nAttempting automatic installation of components [b blue]"
        f"{'[white], [/white]'.join(package_names)}[/b blue]\n"
    )
    args = parse_kv_args(with_args)
    if args:
        rich.print(
            "\n".join(
                [
                    f"\t[green_yellow]{key}[/green_yellow]: [white]{value}[/white]"
                    for key, value in args.items()
                ]
            )
        )
        rich.print("\n\n")

    with Progress(SpinnerColumn(), TextColumn("{task.description}")) as progress:
        if using == "image":
            for package_name in package_names:
                task = progress.add_task(
                    f"Attempting to install [b blue]{package_name}[/b blue] with"
                    " [i yellow]image[/i yellow] installer",
                    total=1,
                )
                try:
                    if "package_name" not in args:
                        args["package_name"] = package_name
                    _install_from_image(**args)
                    args.pop("package_name")
                except Exception as err:
                    progress.update(
                        task,
                        description="[red]✘[/red] Failed with "
                        "[yellow]image[/yellow] installer "
                        f"[blue]({package_name})[/blue]",
                        completed=1,
                    )
                    progress.stop()
                    rich.print("\n\n[red]Installation failled[/red]")
                    rich.print(err)
                    raise err
                    exit(-1)
                else:
                    progress.update(
                        task,
                        description="[green]✔[/green] Success with "
                        f"[yellow]image[/yellow][blue] ({package_name})[/blue]",
                        completed=1,
                    )
            progress.stop()
            rich.print(
                f"\n\nInstallation of [blue]{'[white], [/white]'.join(package_names)}"
                "[/blue] with [yellow]image[/yellow] installer was "
                "[green]successful[/green]"
            )

        elif using == "space":
            for package_name in package_names:
                task = progress.add_task(
                    f"Attempting to install [b blue]{package_name}[/b blue] with"
                    " [i yellow]space[/i yellow] installer",
                    total=1,
                )
                try:
                    if "package_name" not in args:
                        args["package_name"] = package_name
                    args["reverse_id"] = package_name
                    _install_from_space(**args)
                    args.pop("package_name")
                except Exception as err:
                    progress.update(
                        task,
                        description="[red]✘[/red] Failed with "
                        "[yellow]space[/yellow] installer "
                        f"[blue]({package_name})[/blue]",
                        completed=1,
                    )
                    progress.stop()
                    rich.print("\n\n[red]Installation failled[/red]")
                    raise err
                else:
                    progress.update(
                        task,
                        description="[green]✔[/green] Success with "
                        f"[yellow]space[/yellow][blue] ({package_name})[/blue]",
                        completed=1,
                    )
            progress.stop()
            rich.print(
                f"\n\nInstallation of [blue]{'[white], [/white]'.join(package_names)}"
                "[/blue] with [yellow]space[/yellow] installer was "
                "[green]successful[/green]"
            )
        else:
            rich.print(
                f"[red]Installer [yellow]{using}[/yellow] is not supported[/red]"
            )
            exit(-1)

def underscored(entry: str):
    return re.sub(r"[\W\s]", "_", entry)

def install_flow(
    reverse_id: str=typer.Argument(
        ...,
        show_default=False,
        help="Space Flow Reverse ID"
    ),
    deployment_id: str=typer.Option(
        None,
        '--deployment-id',
        '-d',
        show_default=False,
        help='Flow Deployment ID. If not set, will get active version flow.'
    ),
    branch: str=typer.Option(
        None,
        '--branch',
        '-b',
        show_default=False,
        help="Flow branch. If not specified, will take the active one."
    ),
    attach_any: bool=typer.Option(
        False,
        '--attach_any',
        '-a',
        show_default=False,
        help="Attach to any flow deployment"
    )
):
    installer = FlowInstaller()
    attach_to_last = deployment_id is None
    task = Space(
        reverse_id=reverse_id,
        attach_to_any=attach_any,
        attach_to_last=attach_to_last,
        deployment_id=deployment_id,
        branch=branch
    )
    version_name = task.component.version.readable_name
    mappings = {}
    injectables = task.get_injectables()
    for col in injectables:
        mappings[underscored(col.alias)] = col.alias

    integration = Integration(
        mapping=mappings,
        version=version_name,
        deployment=deployment_id,
        injectables=injectables
    )
    try:
        installer.install(
            reverse_id,
            integration
        )
    except Exception as e:
        rich.print(f"Failed to install [yellow]{reverse_id}[/yellow]: [red]{e}[/red]")
        exit(1)
    rich.print(
        f"Successfully installed: [yellow]{reverse_id}[/yellow], "
        f"version [blue]{version_name}[/blue]"
    )
