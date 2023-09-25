import typer

from malevich.install.image import ImageInstaller
from malevich.manifest import ManifestManager

use = typer.Typer()

@use.command('image')
def install_from_image(
    image_ref: str = typer.Argument(
        ...,
        help='Image reference in format <registry>/<image>:<tag>'
    ),
    package_name: str = typer.Argument(
        ...,
        help='Package name'
    ),
    image_auth_user: str =  typer.Argument(
        ...,
        help='Image registry auth user'
    ),
    image_auth_password: str = typer.Argument(
        ...,
        help='Image registry auth password'
    ),
    core_host: str = typer.Option(
        default='https://core.onjulius.co',
        help='Malevich Core hostname'
    ),
    core_user: str = typer.Option(
        default=None,
        help='Malevich Core user'
    ),
    core_token: str = typer.Option(
        default=None,
        help='Malevich Core token'
    )
) -> None:
    installer = ImageInstaller(
        package_name=package_name,
        image_ref=image_ref,
        image_auth=(image_auth_user, image_auth_password),
        core_host=core_host,
        core_auth=(core_user, core_token) if core_user and core_token else None
    )
    manifest_entry = installer.install()
    manifest_manager = ManifestManager()
    manifest_manager.put(
        'dependencies',
        manifest_entry.package_id,
        value=manifest_entry.model_dump()
    )
