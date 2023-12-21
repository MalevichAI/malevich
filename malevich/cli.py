import concurrent.futures
import logging
import re
from typing import Annotated, Optional

import pydantic_yaml as pdyml
import rich
import typer
from malevich_space.cli.cli import app as space_app
from malevich_space.ops import SpaceOps
from malevich_space.schema import HostSchema, Setup, SpaceSetup
from pydantic import ValidationError
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Prompt

from malevich._utility.package import PackageManager

from ._utility.args import parse_kv_args
from .commands.ci import app as ci_app
from .commands.flow import flow as flow_app
from .commands.manifest import app as manifest_app
from .commands.prefs import prefs as prefs
from .commands.use import _install_from_image, _install_from_space
from .commands.use import use as use_app
from .constants import APP_HELP, DEFAULT_CORE_HOST, PROD_SPACE_API_URL
from .help import ci, install, space
from .help import remove as remove_help
from .help import restore as restore_help
from .install.image import ImageInstaller
from .install.installer import Installer
from .install.space import SpaceInstaller
from .manifest import ManifestManager
from .models.manifest import Dependency

logging.getLogger("gql.transport.requests").setLevel(logging.ERROR)
app = typer.Typer(help=APP_HELP, rich_markup_mode="rich")


__With_Args_Help = (
    "Arguments to pass to the installer. "
    "Must be in the format [b]key1=value1,key2=value2[/b] "
    '(" can be used to escape spaces)'
    '\n\n\n'
    'For a particular installer, use [i b] malevich use <installer> --help[/i b] to'
    ' see the list of supported arguments'
)


@app.command("install", help=install["--help"])
def auto_use(
    package_names: Annotated[list[str], typer.Argument(...)],
    using: Annotated[str, typer.Option(help="Installer to use")] = "space",
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
                    exit(-1)
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


def _restore(installer: Installer, depedency: Dependency, progress: Progress) -> None:
    task = None
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
        if task:
            progress.update(
                task,
                completed=True,
                description=f"[red]✘[/red] Package [b blue]{package_id}[/b blue]",
            )
        return e, package_id


@app.command(name="restore", help=restore_help["--help"])
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
                    executor.submit(_restore, image_installer,
                                    dependency, progress)
                )
            elif installed_by == 'space':
                space_installer = SpaceInstaller()
                futures.append(
                    executor.submit(_restore, space_installer,
                                    dependency, progress)
                )
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                progress.stop()
                rich.print(f"[red]Failed to restore package {result[1]}[/red]")
                rich.print(result[0])
                exit(-1)


@space_app.command(help=space["init --help"])
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

# @space_app.command(help=space["login --help"])
# def login() -> None:
#     manf = ManifestManager()
#     api_url = Prompt.ask("Malevich Space API URL", default=PROD_SPACE_API_URL)
#     core_url = Prompt.ask("Malevich Core URL", default=DEFAULT_CORE_HOST)
#     username = Prompt.ask("Username")
#     password = Prompt.ask("Password", password=True)

@space_app.command(help=space["login --help"])
def login(
    api_url: str = PROD_SPACE_API_URL,
    core_url: str = DEFAULT_CORE_HOST,
    space_url: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> None:
    if not space_url:
        domain = re.search(r"\/\/(.*)api\.(.+)\/?", api_url)
        left = domain.group(1) if domain.group(1) else ''
        right = '.' + domain.group(2) if domain.group(2) else ''
        # space_url = f'https://space.{domain}' + ('' if domain.endswith('/') else '/')
        # base_space_url = f'space.{domain}'.rstrip('/')

        space_url = f'https://{left}space{right}/'
        base_space_url = f'{left}space{right}'.rstrip('/')

    manf = ManifestManager()
    rich.print("[b]Welcome to [purple]Malevich Space[/purple]![/b]"
               " The command allows you to connect your account "
               f"to [bright_cyan]{space_url}[/bright_cyan]"
               "[bright_black]\nIf you don't have an account, "
               "please create one and come back.[/bright_black]\n"
               )

    # api_url = Prompt.ask("Malevich Space API URL", default=PROD_SPACE_API_URL)
    # core_url = Prompt.ask("Malevich Core URL", default=DEFAULT_CORE_HOST)
    if not username:
        username = Prompt.ask(
            f"E-mail (or Username) on [bright_cyan]{base_space_url}[/bright_cyan]")
    if not password:
        password = Prompt.ask("Password", password=True)

    setup = SpaceSetup(
        api_url=api_url,
        username=username,
        password=password,
        host=HostSchema(
            conn_url=core_url,
        )
    )

    try:
        SpaceOps(space_setup=setup)
    except Exception as e:
        rich.print(
            f"\n\n[red]Failed to connect to {space_url}. "
            "Please check your credentials and try again.[/red]"
        )
        exit(-1)

    space_password = manf.put_secret("space_password", setup.password)
    setup.password = space_password
    manf.put("space", value=setup)
    rich.print("\nMalevich Space configuration [green]successfully[/green]"
               " added to the manifest\n")


@app.command('remove', help=remove_help['--help'])
def remove(
    package_name: Annotated[str, typer.Argument(...)],
) -> None:
    try:
        manf = ManifestManager()
        manf.remove(
            'dependencies',
            package_name,
        )
        PackageManager().remove_stub(package_name)
        rich.print(f"[green]Package [b]{package_name}[/b] removed[/green]")
        rich.print(f"Bye, bye [b]{package_name}[/b]")
    except Exception as e:
        rich.print(
            f"[red]Failed to remove package [b]{package_name}[/b][/red]")
        rich.print(e)
        exit(-1)


space_app.add_typer(flow_app, name="flow")

app.add_typer(use_app, name="use")
app.add_typer(manifest_app, name="manifest")
app.add_typer(space_app, name="space", help=space["--help"])
app.add_typer(ci_app, name="ci", help=ci["--help"])
app.add_typer(prefs, name="prefs")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
