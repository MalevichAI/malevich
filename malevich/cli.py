import concurrent.futures
import logging
from typing import Annotated

import pydantic_yaml as pdyml
import rich
import typer
from malevich_space.cli.cli import app as space_app
from malevich_space.schema import SpaceSetup
from pydantic import ValidationError
from rich.progress import Progress, SpinnerColumn, TextColumn

from ._utility.args import parse_kv_args
from .commands.manifest import app as manifest_app
from .commands.use import install_from_image
from .commands.use import use as use_app
from .constants import APP_HELP
from .install.image import ImageInstaller
from .install.installer import Installer
from .manifest import ManifestManager
from .models.manifest import Dependency

logging.getLogger("gql.transport.aiohttp").setLevel(logging.ERROR)
app = typer.Typer(help=APP_HELP, rich_markup_mode="rich")

__With_Args_Help = (
    "Arguments to pass to the installer. "
    "Must be in the format [code]key1=value1,key2=value2[/code] "
    '(" can be used to escape spaces)'
)


@app.command("install", help="Install multiple packages using the image installer")
def auto_use(
    package_names: Annotated[list[str], typer.Argument(...)],
    using: Annotated[str, typer.Option(help="Installer to use")] = "image",
    with_args: Annotated[str, typer.Option(help=__With_Args_Help)] = "",
) -> None:
    rich.print(
        "\n\nAttempting automatic installation of packages [b blue]"
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
                    install_from_image(**args)
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
        else:
            rich.print(
                f"[red]Installer [yellow]{using}[/yellow] is not supported[/red]"
            )
            exit(-1)


def _restore(installer: Installer, depedency: Dependency, progress: Progress) -> None:
    try:
        package_id = depedency["package_id"]
        parsed = installer.construct_dependency(depedency)
        task = progress.add_task(f"Package [green]{package_id}[/green] with "
                                f"[yellow]{installer.name}[/yellow]", total=1)
        installer.restore(parsed)
        progress.update(
            task,
            completed=True,
            description=f"[green]✔[/green] Package [b blue]{package_id}[/b blue]",
        )
    except Exception as e:
        progress.update(
            task,
            completed=True,
            description=f"[red]✘[/red] Package [b blue]{package_id}[/b blue]",
        )
        return e, package_id


@app.command(name="restore")
def restore() -> None:
    manf = ManifestManager()
    image_installer = ImageInstaller()
    with Progress(
        SpinnerColumn(), TextColumn("{task.description}")
    ) as progress, concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for record in manf.query("dependencies"):
            key = [*record.keys()][0]
            dependency = record[key]
            installed_by = dependency["installer"]
            if installed_by == "image":
                futures.append(
                    executor.submit(_restore, image_installer, dependency, progress)
                )
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                progress.stop()
                rich.print(f"[red]Failed to restore package {result[1]}[/red]")
                rich.print(result[0])
                exit(-1)


@space_app.command()
def init(path_to_setup: Annotated[str, typer.Argument(...)]) -> None:
    try:
        setup = pdyml.parse_yaml_file_as(SpaceSetup, path_to_setup)
    except ValidationError as err:
        rich.print(
            f"[b red]Setup file [white]{path_to_setup}[/white] is not a correct configuration[/b red]\n"  # noqa: E501
        )  # noqa: E501
        for _err in err.errors():
            rich.print(f"\t- {_err['msg']} at [pink]{_err['loc'][0]}[pink]")
        rich.print("\n[red]Could not initialize Space installer[/red]")
    else:
        manf = ManifestManager()

        setup.password = manf.put_secret("space_password", setup.password)
        ManifestManager().put("space", value=setup)

        rich.print(
            "\nMalevich Space configuration [green]successfully[/green] added to the manifest\n"  # noqa: E501
        )  # noqa: E501


app.add_typer(use_app, name="use")
app.add_typer(manifest_app, name="manifest")
app.add_typer(space_app, name="space")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
