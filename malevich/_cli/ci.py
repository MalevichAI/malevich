import re

import rich
import typer
from rich.prompt import Prompt

from malevich._utility.ci.github import DockerRegistry, GithubCIOps
from malevich.constants import IMAGE_BASE
from malevich.constants import SPACE_API_URL as DEFAULT_SPACE_HOST

github = typer.Typer()


@github.command(help="Initialize the Github CI/CD pipeline")
def init(
    interactive: bool = typer.Option(
        False,
        help="Run the initialization wizard",
        show_default=False,
    ),
    repo_name: str = typer.Option(
        None,
        help="Github repository name in the format <username>/<repo-name>",
        show_default=False,
    ),
    github_token: str = typer.Option(
        None,
        help="Github token with access to the repository",
        show_default=False,
    ),
    space_user: str = typer.Option(
        None,
        help="Malevich Space username",
        show_default=False,
    ),
    space_token: str = typer.Option(
        None,
        help="Malevich Space token (password)",
        show_default=False,
    ),
    space_url: str = typer.Option(
        None,
        help="Malevich Space API URL",
        show_default=False,
    ),
    branch: str = typer.Option(
        None,
        help="Branch to setup CI in",
        show_default=False,
    ),
    registry_url: str = typer.Option(
        'ghcr.io',
        help="Docker Image Registry URL, for example `public.ecr.aws` or 'cr.yandex'",
        show_default=False,
    ),
    registry_id: str = typer.Option(
        'owner',
        help="Docker Registry ID",
        show_default=False,
    ),

    image_user: str = typer.Option(
        'USERNAME',
        help="Username to access the Docker Image Registry",
        show_default=False,
    ),
    image_token: str = typer.Option(
        'token',
        help="Password to access the Docker Image Registry",
        show_default=False,
    ),
    org_id: str = typer.Option(
        'empty',
        help="Malevich space organization ID",
        show_default=False
    )
):
    """Initialize the Github CI/CD pipeline."""
    rich.print("Welcome to the Github CI/CD pipeline initialization wizard!")
    ops = GithubCIOps()

    if interactive:
        # Get the repository name
        repo_name = Prompt.ask(
            "Enter the Github repository name in the format "
            "[pale_green1]<username>/<repo-name>[/pale_green1]"
        )

        if not re.match(r"^.*/.*", repo_name):
            rich.print(
                "[red]Invalid repository name format. Please try again.[/red]"
            )
            return
        # Get Github token
        github_token = Prompt.ask(
            "Enter the Github token with access to the repository "
            "[i bright_black](ghp_...)[/i bright_black]"
        )

        space_user = Prompt.ask(
            "Enter the username to access [b deep_pink3]Malevich Space[/b deep_pink3]"
            " [i bright_black]"
            "(leave empty to use access token instead)[/i bright_black]"
        )

        space_token = Prompt.ask(
            f"Enter the {'password' if space_user else 'token'} to access "
            "[b deep_pink3]Malevich Space[/b deep_pink3]"
        )

        space_url = Prompt.ask(
            "Enter the host of [b deep_pink3]Malevich Space[/b deep_pink3] ",
            default=DEFAULT_SPACE_HOST
        )

        branch = Prompt.ask(
            "Enter the branch to run the pipeline on",
            default="main"
        )

        registry_type = Prompt.ask(
            "Enter the Docker Container Registry type",
            choices=[str(x.value) for x in DockerRegistry],
            default=DockerRegistry.PUBLIC_AWS_ECR,
        )

        if registry_type != DockerRegistry.PRIVATE_AWS_ECR:
            registry_url = Prompt.ask(
                "Enter the Docker Container Registry URL ",
                default=IMAGE_BASE.split('/')[0]
            )

        registry_id = Prompt.ask(
            "Enter the Docker Container Registry ID ",
            default=IMAGE_BASE.split('/')[1]
        )

        image_user = Prompt.ask(
            "Enter the username to access the Docker Container Registry",
        )

        image_token = Prompt.ask(
            "Enter the password to access the Docker Container Registry",
        )

    rich.print("Calling the Github API to setup the CI/CD pipeline")

    ops.setup_ci(
        token=github_token,
        repository=repo_name,
        space_user=space_user,
        space_token=space_token,
        space_url=space_url,
        branch=branch,
        registry_url=registry_url,
        registry_id=registry_id,
        image_user=image_user,
        image_token=image_token,
        org_id=org_id,
        verbose=True
    )

app = typer.Typer()
app.add_typer(github, name="github", help="Github CI/CD pipeline")
