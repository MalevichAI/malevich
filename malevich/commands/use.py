from typing import Annotated, Optional

import rich
import typer
from rich.progress import Progress, SpinnerColumn, TextColumn

from malevich.install.space import SpaceInstaller

from ..constants import (
    DEFAULT_CORE_HOST,
    IMAGE_BASE,
)
from ..help import use as help
from ..install.image import ImageInstaller
from ..manifest import ManifestManager

use = typer.Typer(help=help['--help'], rich_markup_mode="rich")


def _install_from_image(
    package_name: str,
    image_ref: Optional[str] = None,
    image_auth_user: Optional[str] = None,
    image_auth_password: Optional[str] = None,
    core_host: str = DEFAULT_CORE_HOST,
    core_user: Optional[str] = None,
    core_token: Optional[str] = None,
    progress: Progress = None,
) -> None:
    """UI-friendly wrapper for image installer

    Install package from image registry and renders CLI interface
    for the process of installation.

    Use `~ImageInstaller.install` method to directly install package

    Args:
        package_name (str): Name of the package
        image_ref (Optional[str], optional): Image reference in format
            <registry>/<image>:<tag>. Defaults to None.
        image_auth_user (Optional[str], optional): Image registry auth user.
            Defaults to None.
        image_auth_password (Optional[str], optional): Image registry auth password.
            Defaults to None.
        core_host (str, optional): Malevich Core hostname.
            Defaults to DEFAULT_CORE_HOST.
        core_user (Optional[str], optional): Malevich Core user. Defaults to None.
        core_token (Optional[str], optional): Malevich Core token. Defaults to None.
        progress (Progress, optional): Rich progress bar. Defaults to None.

    Raises:
        Exception: If package with the same name already exists in manifest
            with different installer
    """
    if image_ref is None:
        return _install_from_image(
            package_name=package_name,
            image_ref=IMAGE_BASE.format(app=package_name),
            image_auth_user=image_auth_user,
            image_auth_password=image_auth_password,
            core_host=core_host or DEFAULT_CORE_HOST,
            core_user=core_user,
            core_token=core_token,
            progress=Progress(
                SpinnerColumn(), TextColumn("{task.description}")),
        )
    installer = ImageInstaller()
    if progress:
        task_ = progress.add_task(
            f"Attempting to install [b blue]{package_name}[/b blue] with"
            " [i yellow]image[/i yellow] installer",
            total=1,
        )

    try:
        manifest_entry = installer.install(
            package_name=package_name,
            image_ref=image_ref,
            image_auth=(image_auth_user, image_auth_password),
            core_host=core_host,
            core_auth=(
                core_user, core_token) if core_user and core_token else None,
        )

        manifest_manager = ManifestManager()
        if entry := manifest_manager.query("dependencies", package_name):
            if entry.get("installer") != "image":
                raise Exception(
                    f"Package {package_name} already exists with different installer."
                    "\nPossible solutions:"
                    "\n\t- Remove package from manifest and try again. Use `malevich remove` command"  # noqa: E501
                    "\n\t- Use `malevich restore` command to restore package with its installer"  # noqa: E501
                )

            manifest_manager.put(
                "dependencies",
                package_name,
                value=manifest_entry.model_dump(),
            )
            if progress:
                progress.update(
                    task_,
                    description="\n[green]✔[/green] Package "
                    f"[blue]({package_name})[/blue] "
                    "[yellow]updated[/yellow] successfully",
                    completed=1,
                )
        else:
            if progress:
                progress.update(
                    task_,
                    description="\n[green]✔[/green] Package "
                    f"[blue]({package_name})[/blue] "
                    "installed successfully",
                    completed=1,
                )
            manifest_manager.put(
                "dependencies",
                value={f"{package_name}": manifest_entry.model_dump()},
                append=True,
            )

        manifest_manager.save()
        return True
    except Exception as err:
        if progress:
            progress.update(
                task_,
                description="\n[red]✘[/red] Failed with "
                "[yellow]image[/yellow] installer "
            )
        raise err


def _install_from_space(
    package_name: str,
    reverse_id: str,
    branch: Optional[str] = None,
    version: Optional[str] = None,
    progress: Progress = None,
) -> None:
    installer = SpaceInstaller()
    if progress:
        task_ = progress.add_task(
            f"Attempting to install [b blue]{package_name}[/b blue] with"
            " [i yellow]space[/i yellow] installer",
            total=1,
        )

    try:
        manifest_entry = installer.install(
            package_name=package_name,
            reverse_id=reverse_id,
            branch=branch,
            version=version,
        )
        manifest_manager = ManifestManager()
        if entry := manifest_manager.query("dependencies", package_name):
            if entry.get("installer") != "space":
                raise Exception(
                    f"Package {package_name} already exists with different installer."
                    "\nPossible solutions:"
                    "\n\t- Remove package from manifest and try again. Use `malevich remove` command"  # noqa: E501
                    "\n\t- Use `malevich restore` command to restore package with its installer"  # noqa: E501
                )

            manifest_manager.put(
                "dependencies",
                package_name,
                value=manifest_entry.model_dump(),
            )
            if progress:
                progress.update(
                    task_,
                    description="\n[green]✔[/green] Package "
                    f"[blue]({package_name})[/blue] "
                    "[yellow]updated[/yellow] successfully",
                    completed=1,
                )
        else:
            if progress:
                progress.update(
                    task_,
                    description="\n[green]✔[/green] Package "
                    f"[blue]({package_name})[/blue] "
                    "installed successfully",
                    completed=1,
                )
            manifest_manager.put(
                "dependencies",
                value={f"{package_name}": manifest_entry.model_dump()},
                append=True,
            )

        manifest_manager.save()
        return True
    except Exception as err:
        if progress:
            progress.update(
                task_,
                description="\n[red]✘[/red] Failed with "
                "[yellow]space[/yellow] installer "
            )
        raise err


@use.command("image", help=help['image --help'])
def install_from_image(
    package_name: Annotated[
        str,
        typer.Argument(help="Package name")
    ],
    image_ref: Annotated[
        Optional[str],
        typer.Argument(
            help="Image reference in format <registry>/<image>:<tag>"
        )
    ] = None,
    image_auth_user: Annotated[
        str,
        typer.Argument(
            help="Image registry auth user"
        )
    ] = "",
    image_auth_password: Annotated[
        str,
        typer.Argument(
            help="Image registry auth password"
        )
    ] = "",
    core_host: Annotated[
        str,
        typer.Option(
            help="Malevich Core hostname"
        )
    ] = DEFAULT_CORE_HOST,
    core_user: Annotated[
        Optional[str],
        typer.Option(
            help="Malevich Core user"
        )
    ] = None,
    core_token: Annotated[
        Optional[str],
        typer.Option(
            help="Malevich Core token"
        )
    ] = None,
) -> None:
    try:
        if image_ref is None:
            return _install_from_image(
                package_name=package_name,
                image_ref=IMAGE_BASE.format(app=package_name),
                image_auth_user=image_auth_user,
                image_auth_password=image_auth_password,
                core_host=core_host or DEFAULT_CORE_HOST,
                core_user=core_user,
                core_token=core_token,
                progress=Progress(
                    SpinnerColumn(), TextColumn("{task.description}")),
            )
        else:
            return _install_from_image(
                package_name=package_name,
                image_ref=image_ref,
                image_auth_user=image_auth_user,
                image_auth_password=image_auth_password,
                core_host=core_host or DEFAULT_CORE_HOST,
                core_user=core_user,
                core_token=core_token,
                progress=Progress(
                    SpinnerColumn(), TextColumn("{task.description}")),
            )
    except Exception as err:
        rich.print("\n\n[red]Installation failled[/red]")
        rich.print(err)
        exit(-1)


@use.command("space", help=help['space --help'])
def install_from_space(
    package_name: Annotated[
        str,
        typer.Argument(help="Package name")
    ],
    reverse_id: Annotated[
        str,
        typer.Argument(help="Reverse id of the component")
    ],
    branch: Annotated[
        Optional[str],
        typer.Argument(help="Branch of the component")
    ] = None,
    version: Annotated[
        Optional[str],
        typer.Argument(help="Version of the component")
    ] = None,
) -> None:
    try:
        return _install_from_space(
            package_name=package_name,
            reverse_id=reverse_id,
            branch=branch,
            version=version,
            progress=Progress(
                SpinnerColumn(), TextColumn("{task.description}")),
        )
    except Exception as err:
        rich.print("\n\n[red]Installation failled[/red]")
        rich.print(err)
        exit(-1)
