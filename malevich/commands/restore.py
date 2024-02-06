import concurrent.futures

import rich
from rich.progress import Progress, SpinnerColumn, TextColumn

from ..commands.prefs import prefs as prefs
from ..install.image import ImageInstaller
from ..install.installer import Installer
from ..install.space import SpaceInstaller
from ..manifest import ManifestManager


def _restore(installer: Installer, depedency: dict, progress: Progress, task) -> None:
    try:
        package_id = depedency["package_id"]
        parsed = installer.construct_dependency(depedency)

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


# @app.command(name="restore", help=restore_help["--help"])
def restore() -> None:
    manf = ManifestManager()
    image_installer = ImageInstaller()
    with Progress(
        SpinnerColumn(), TextColumn("{task.description}")
    ) as progress, concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for record in manf.query("dependencies"):
            key = next(iter(record.keys()))
            dependency = record[key]
            installed_by = dependency["installer"]
            task = progress.add_task(
                f"Package [green]{dependency['package_id']}[/green] with "
                f"[yellow]{installed_by}[/yellow]",
                total=1
            )
            if installed_by == "image":
                futures.append(
                    executor.submit(_restore, image_installer,
                                    dependency, progress, task)
                )
            elif installed_by == 'space':
                space_installer = SpaceInstaller()
                futures.append(
                    executor.submit(_restore, space_installer,
                                    dependency, progress, task)
                )
    for future in concurrent.futures.as_completed(futures):
        result = future.result()
        if result:
            progress.stop()
            rich.print(f"[red]Failed to restore package {result[1]}[/red]")
            rich.print(result[0])
            exit(-1)
