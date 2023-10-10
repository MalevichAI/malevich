import rich
from rich.progress import Progress

import typer

from malevich.constants import DEFAULT_CORE_HOST, USE_HELP, USE_IMAGE_HELP
from malevich.install.image import ImageInstaller
from malevich.manifest import ManifestManager

use = typer.Typer(help=USE_HELP, rich_markup_mode="rich")

@use.command('image', help=USE_IMAGE_HELP)
def install_from_image(
    package_name: str = typer.Argument(
        ...,
        help='Package name'
    ),
    image_ref: str = typer.Argument(
        None,
        help='Image reference in format <registry>/<image>:<tag>'
    ),
    image_auth_user: str =  typer.Argument(
        None,
        help='Image registry auth user'
    ),
    image_auth_password: str = typer.Argument(
        None,
        help='Image registry auth password'
    ),
    core_host: str = typer.Option(
        default=DEFAULT_CORE_HOST,
        help='Malevich Core hostname'
    ),
    core_user: str = typer.Option(
        default=None,
        help='Malevich Core user'
    ),
    core_token: str = typer.Option(
        default=None,
        help='Malevich Core token'
    ),
) -> None:
    installer = ImageInstaller()
    manifest_entry = installer.install(
        package_name=package_name,
        image_ref=image_ref,
        image_auth=(image_auth_user, image_auth_password),
        core_host=core_host,
        core_auth=(core_user, core_token) if core_user and core_token else None
    )
    manifest_manager = ManifestManager()
    if manifest_manager.query('dependencies', package_name):
        manifest_manager.put(
            'dependencies',
            package_name,
            value=manifest_entry.model_dump(),

        )
    else:
        manifest_manager.put(
            'dependencies',
            value={
                f'{package_name}': manifest_entry.model_dump()
            },
            append=True
        )

@use.command(name='restore')
def restore() -> None:
    manf = ManifestManager()
    image_installer = ImageInstaller()
    with Progress() as progress:
        for record in manf.query('dependencies'):
            key = [*record.keys()][0]
            dependency = record[key]
            task = progress.add_task(
                f"Package [green]{dependency['package_id']}[/green]", total=1
            )
            installed_by = dependency['installer']
            if installed_by == 'image':
                parsed = image_installer.construct_dependency(
                    dependency
                )
                parsed.options.image_auth_pass = manf.query_secret(
                    parsed.options.image_auth_pass
                )
                parsed.options.image_auth_user = manf.query_secret(
                    parsed.options.image_auth_user
                )
                parsed.options.core_auth_user = manf.query_secret(
                    parsed.options.core_auth_user
                )
                parsed.options.core_auth_token = manf.query_secret(
                    parsed.options.core_auth_token
                )
                parsed.options.core_host = manf.query_secret(
                    parsed.options.core_host
                )
                parsed.options.image_ref = manf.query_secret(
                    parsed.options.image_ref
                )

                if not all([
                    parsed.options.image_auth_pass,
                    parsed.options.image_auth_user,
                    parsed.options.core_auth_user,
                    parsed.options.core_auth_token,
                    parsed.options.core_host,
                    parsed.options.image_ref
                ]):
                    raise Exception(
                        "It seems you are missing a number of secrets. Please "
                        "fix it using command `malevich manifest secrets fix`"
                    )


                image_installer.restore(parsed)
            progress.advance(task, 1)

