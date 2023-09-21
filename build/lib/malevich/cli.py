import tempfile
from typing import Annotated

import typer

from malevich.utility.git.clone import clone_python_files
from malevich.utility.scan import scan

app = typer.Typer()


@app.command(name="use")
def use(
    link: Annotated[
        str, typer.Argument(..., help="Link to Git repository or Docker image")
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
) -> None:
    """Scan a Git repository or Docker image for Malevich elements.

    Helps you to acquire Malevich elements from a Git repository or Docker image and
    make them available for use in your project.
    """
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
    else:
        # TODO: Implement Docker image support
        pass


@app.command()
def cache() -> None:
    # TODO: Implement cache command
    pass


def main() -> None:
    app()


if __name__ == "__main__":
    main()
