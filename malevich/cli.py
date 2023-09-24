import tempfile
from typing import Annotated

import typer

from malevich._utility.ask_core.image_info import get_image_info
from malevich._utility.git.clone import clone_python_files
from malevich._utility.scan import scan

# from malevich.install.create import create_processors
# from malevich.install.mimic import mimic_package

app = typer.Typer()


@app.command(name="use")
def use(
    link: Annotated[
        str, typer.Argument(..., help="Link to Git repository or Docker image")
    ],
    alias: Annotated[
        str,
        typer.Argument(
            ...,
            help="Alias that will be used to refer to the acquired elements",
        ),
    ],
    username: Annotated[
        str,
        typer.Option(
            "--username",
            "-u",
            help="Username for Git repository or Docker registry",
        ),
    ] = None,
    password_token: Annotated[
        str,
        typer.Option(
            "--password",
            "-p",
            "--token",
            "-t",
            help="Password or token for Git repository or Docker registry",
        ),
    ] = None,
    clone_to: Annotated[
        str,
        typer.Option(
            "--clone-to",
            "-c",
            help="Folder to clone into. Defaults to a temporary directory.",
        ),
    ] = tempfile.TemporaryDirectory().name,
    branch: Annotated[
        str,
        typer.Option(
            "--branch",
            "-b",
            help="Branch to clone. Defaults to the default branch.",
        ),
    ] = None,
    filter: Annotated[
        str,
        typer.Option(
            "--filter",
            "-f",
            help="Filter to apply to the files. Defaults to `*.py`.",
        ),
    ] = "*.py",
    core_host: Annotated[
        str,
        typer.Option(
            "--core-host",
            help="Core host to use for acquiring Malevich elements.",
        ),
        # TODO: Default core host
    ] = None,
    core_username: Annotated[
        str,
        typer.Option(
            "--core-username",
            help="Core username to use for acquiring Malevich elements.",
        ),
    ] = None,
    core_password_token: Annotated[
        str,
        typer.Option(
            "--core-password",
            "--core-token",
            help="Core password or token to use for acquiring Malevich elements.",
        ),
    ] = None,
) -> None:
    """Scan a Git repository or Docker image for Malevich elements.

    Helps you to acquire Malevich elements from a Git repository or Docker image and
    make them available for use in your project.
    """
    # TODO: Implement install with Space
    if link.startswith("git") or link.startswith("http"):
        files = clone_python_files(
            link,
            auth=(username, password_token),
            branch=branch,
            folder=clone_to,
        )
        typer.echo(f"Cloned {len(files)} Python files to {clone_to}")
        processors, inputs, outputs, inits = [], [], [], []
        for file in files:
            _processors, _inputs, _outputs, _inits = scan(file)
            processors.extend(_processors)
            inputs.extend(_inputs)
            outputs.extend(_outputs)
            inits.extend(_inits)
        total_count = len(processors) + len(inputs) + len(outputs) + len(inits)
        typer.echo(f"Found {total_count} elements, including:")
        typer.echo(f"\t{len(processors)} processors")
        typer.echo(f"\t{len(inputs)} inputs")
        typer.echo(f"\t{len(outputs)} outputs")
        typer.echo(f"\t{len(inits)} inits")

        # TODO: Install using GitInstaller
    else:
        if not (core_host and core_username and core_password_token):
            raise typer.BadParameter(
                "Core host, username and password/token are required for Docker images"
            )
        typer.echo(f"Acquiring image info from {link}")
        info = get_image_info(
            link,
            image_auth=(username, password_token),
            core_auth=(core_username, core_password_token),
            host=core_host,
        )
        typer.echo(f"Found {len(info['processors'])} processors")
        processors = []
        for name, processor in info['processors'].items():
            typer.echo(f"Found processor {processor['id']}")
            args = [(arg, 'DFS' in arg[1]) for arg in processor['arguments']]
            processors.append((name, args))
        # typer.echo("Creating metascripts...")
        # meta = create_processors(
        #     [name for name, _ in processors],
        #     link,
        #     (username, password_token),
        #     [args for _, args in processors],
        # )
        # typer.echo("Installing package...")
        # mimic_package(alias, meta)
        # TODO: Install using DockerInstaller




@app.command()
def cache() -> None:
    # TODO: Implement cache command
    pass


def main() -> None:
    app()


if __name__ == "__main__":
    main()
