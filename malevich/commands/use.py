from typing import Annotated

import typer

from ..constants import DEFAULT_CORE_HOST, IMAGE_BASE, USE_HELP, USE_IMAGE_HELP
from ..install.image import ImageInstaller
from ..manifest import ManifestManager

use = typer.Typer(help=USE_HELP, rich_markup_mode="rich")


@use.command("image", help=USE_IMAGE_HELP)
def install_from_image(
    package_name: Annotated[str, typer.Argument(help="Package name")],
    image_ref: Annotated[
        str, typer.Argument(help="Image reference in format <registry>/<image>:<tag>")
    ] = None,
    image_auth_user: Annotated[
        str, typer.Argument(help="Image registry auth user")
    ] = "",
    image_auth_password: Annotated[
        str, typer.Argument(help="Image registry auth password")
    ] = "",
    core_host: Annotated[
        str, typer.Option(help="Malevich Core hostname")
    ] = DEFAULT_CORE_HOST,
    core_user: Annotated[str, typer.Option(help="Malevich Core user")] = None,
    core_token: Annotated[str, typer.Option(help="Malevich Core token")] = None,
) -> None:
    if image_ref is None:
        return install_from_image(
            package_name=package_name,
            image_ref=IMAGE_BASE.format(app=package_name),
            image_auth_user=image_auth_user,
            image_auth_password=image_auth_password,
            core_host=core_host or DEFAULT_CORE_HOST,
            core_user=core_user,
            core_token=core_token,
        )

    installer = ImageInstaller()
    manifest_entry = installer.install(
        package_name=package_name,
        image_ref=image_ref,
        image_auth=(image_auth_user, image_auth_password),
        core_host=core_host,
        core_auth=(core_user, core_token) if core_user and core_token else None,
    )
    manifest_manager = ManifestManager()
    if manifest_manager.query("dependencies", package_name):
        manifest_manager.put(
            "dependencies",
            package_name,
            value=manifest_entry.model_dump(),
        )
    else:
        manifest_manager.put(
            "dependencies",
            value={f"{package_name}": manifest_entry.model_dump()},
            append=True,
        )
