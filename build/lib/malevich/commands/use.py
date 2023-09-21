import typer
from typing import Annotated
from malevich.utility.git.clone import clone_python_files

app = typer.Typer()

@app.command()
def use(
    link: Annotated[
        str, typer.Argument(..., help="Link to Git repository or Docker image")
    ],
    username: Annotated[
        str,
        typer.Option(
            None,
            "--username",
            "-u",
            help="Username for Git repository or Docker registry",
        ),
    ] = None,
    password_token: Annotated[
        str,
        typer.Option(
            None,
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
            None,
            "--clone-to",
            "-c",
            help="Folder to clone into. Defaults to a temporary directory.",
        ),
    ] = None,
    branch: Annotated[
        str,
        typer.Option(
            None,
            "--branch",
            "-b",
            help="Branch to clone. Defaults to the default branch.",
        ),
    ] = None,
    
):
    if link.startswith("git") or link.startswith('http'):
        files = clone_python_files(
            link,
            auth=(username, password_token),
            branch=branch,
            folder=clone_to,
        )   
        typer.echo(f"Cloned {len(files)} Python files to {clone_to}")
        
    else:
        pass

    