import logging
from typing import Annotated

import pydantic_yaml as pdyml
import rich
import typer
from malevich_space.cli.cli import app as space_app
from malevich_space.schema import SpaceSetup
from pydantic import ValidationError
from rich.progress import Progress, SpinnerColumn, TextColumn

from .commands.manifest import app as manifest_app
from .commands.use import install_from_image
from .commands.use import use as use_app
from .constants import APP_HELP
from .install.image import ImageInstaller
from .manifest import ManifestManager

logging.getLogger("gql.transport.aiohttp").setLevel(logging.ERROR)
app = typer.Typer(help=APP_HELP, rich_markup_mode="rich")

@app.command('install')
def auto_use(package_name: str) -> None:
    rich.print(f"\n\nAttempting automatic installation of package [b blue]{package_name}[/b blue]\n")  # noqa: E501
    with Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
    ) as progress:
        task = progress.add_task(
            f"Attempting to install [b blue]{package_name}[/b blue] with [i yellow]image[/i yellow] installer", total=1  # noqa: E501
        )
        try:
            install_from_image(package_name=package_name)
        except Exception:
            # BUG: This is not a good way to handle errors
            progress.update(
                task,
                description="[red]✘[/red] Failed with [yellow]image[/yellow] installer",
                completed=1
            )
            progress.stop()
            rich.print("\n\n[red]Installation failled[/red]")
        else:
            progress.update(
                task,
                description="[green]✔[/green] Success with [yellow]image[/yellow]",
                completed=1
            )
            progress.stop()
            rich.print(f"\n\nInstallation of [blue]{package_name}[/blue] with [yellow]image[/yellow] installer was [green]successful[/green]")  # noqa: E501
        return


@app.command(name="restore")
def restore() -> None:
    manf = ManifestManager()
    image_installer = ImageInstaller()
    with Progress(SpinnerColumn(), TextColumn("{task.description}")) as progress:
        for record in manf.query("dependencies"):
            key = [*record.keys()][0]
            dependency = record[key]
            task = progress.add_task(
                f"Package [green]{dependency['package_id']}[/green]", total=1
            )
            installed_by = dependency["installer"]
            if installed_by == "image":
                parsed = image_installer.construct_dependency(dependency)
                parsed.options.image_auth_pass = manf.query_secret(
                    parsed.options.image_auth_pass,
                    only_value=True,
                )
                parsed.options.image_auth_user = manf.query_secret(
                    parsed.options.image_auth_user,
                    only_value=True,
                )
                parsed.options.core_auth_user = manf.query_secret(
                    parsed.options.core_auth_user,
                    only_value=True,
                )
                parsed.options.core_auth_token = manf.query_secret(
                    parsed.options.core_auth_token,
                    only_value=True,
                )

                # if not all(
                #     [
                #         parsed.options.image_auth_pass,
                #         parsed.options.image_auth_user,
                #         parsed.options.core_auth_user,
                #         parsed.options.core_auth_token,
                #     ]
                # ):
                #     rich.print(
                #         "\n[red bold]Error[/red bold]: \n"
                #         "\tIt seems you are missing a number of secrets. Please "
                #         "fix it using command [code]malevich manifest secrets restore[/code]\n\n"  # noqa: E501
                #     )
                #     quit(-1)

                # if not parsed.options.image_ref:
                #     rich.print(
                #         "\n[red bold]Error[/red bold]: \n"
                #         "\tMissing [code]image_ref[/code] variable. Please"
                #         "\tfix it manually "  # noqa: E501
                #     )
                image_installer.restore(parsed)
            progress.advance(task, 1)

@space_app.command()
def init(path_to_setup: Annotated[str, typer.Argument(...)]) -> None:
    try:
        setup = pdyml.parse_yaml_file_as(SpaceSetup, path_to_setup)
    except ValidationError as err:
        rich.print(f"[b red]Setup file [white]{path_to_setup}[/white] is not a correct configuration[/b red]\n")  # noqa: E501
        for _err in err.errors():
            rich.print(f"\t- {_err['msg']} at [pink]{_err['loc'][0]}[pink]")
        rich.print("\n[red]Could not initialize Space installer[/red]")
    else:
        manf = ManifestManager()

        setup.password = manf.put_secret('space_password', setup.password)
        ManifestManager().put('space', value=setup)

        rich.print('\nMalevich Space configuration [green]successfully[/green] added to the manifest\n')  # noqa: E501


app.add_typer(use_app, name="use")
app.add_typer(manifest_app, name="manifest")
app.add_typer(space_app, name='space')

def main() -> None:
    app()


if __name__ == "__main__":
    main()
