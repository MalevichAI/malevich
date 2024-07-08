import concurrent.futures

import rich
from rich.progress import Progress, SpinnerColumn, TextColumn

from malevich._utility import FlowStub
from malevich.install import (
    ImageInstaller,
    Installer,
    SpaceInstaller,
)
from malevich.manifest import ManifestManager
from malevich.models.dependency import Integration

from .prefs import prefs as prefs


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
                image_installer = ImageInstaller()
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

    progress.stop()
    rich.print("\n\n[b]Restoring integrations:[/b]")
    for flow, integrations in manf.query('flows').items():
        FlowStub.sync_flows(flow, integrations=[
            Integration(**i) for i in integrations
        ])
        rich.print(
            f"[green]✔[/green] [green]{flow}[/green] ({len(integrations)} integrations)"
        )
