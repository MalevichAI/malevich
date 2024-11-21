import re
from typing import Optional

import rich
import typer
from malevich_space.ops import SpaceOps
from malevich_space.schema import HostSchema, SpaceSetup
from rich.prompt import Prompt

from malevich.constants import DEFAULT_CORE_HOST, PROD_SPACE_API_URL
from malevich.manifest import ManifestManager

from ..prefs import prefs as prefs


def login(
    no_input: bool = typer.Option(False, "--no-input"),
    api_url: str = PROD_SPACE_API_URL,
    core_url: str = DEFAULT_CORE_HOST,
    space_url: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    org_id: Optional[str] = None,
) -> None:
    try:
        if not space_url:
            domain = re.search(r"\/\/(.*)api\.(.+)\/?", api_url)
            left = domain.group(1) if domain.group(1) else ''
            right = '.' + domain.group(2) if domain.group(2) else ''
            space_url = f'https://{left}space{right}/'
            base_space_url = f'https://{left}space{right}'.rstrip('/')
        else:
            domain = re.search(r"\/\/(.*)space\.(.+)\/?", space_url)
            left = domain.group(1) if domain.group(1) else ''
            right = '.' + domain.group(2) if domain.group(2) else ''
            base_space_url = f'{left}space{right}'.rstrip('/')
            api_url = f'https://{left}api{right.rstrip("/")}/'
    except Exception:
        base_space_url = api_url
    if no_input and (username is None or password is None):
        rich.print("[red]You have to set --username and --password parameters, "
                   "if --no-input is used[/red]")
        return False

    manf = ManifestManager()
    rich.print(
        "[b]Welcome to [purple]Malevich Space[/purple]![/b]"
        " The command allows you to connect your account "
        f"to [bright_cyan]{space_url}[/bright_cyan]"
        "[bright_black]\nIf you don't have an account, "
        "please create one and come back.[/bright_black]\n"
    )

    if not username:
        username = Prompt.ask(
            f"E-mail (or Username) on [bright_cyan]{base_space_url}[/bright_cyan]"
        )
    if not password:
        password = Prompt.ask(
            "Password",
            password=True
        )

    if not org_id and not no_input:
        org_id = Prompt.ask(
            "Organization slug (leave blank to use personal account)",
            default=None,
        )

    setup = SpaceSetup(
        api_url=api_url.rstrip('/'),
        username=username,
        password=password,
        org=org_id,
        host=HostSchema(
            conn_url=core_url,
        )
    )

    try:
        SpaceOps(space_setup=setup)
    except Exception:
        rich.print(
            f"\n\n[red]Failed to connect to {space_url}. "
            "Please check your credentials and try again.[/red]"
        )
        return False

    space_password = manf.put_secret("space_password", setup.password)
    setup.password = space_password
    manf.put("space", value=setup)
    rich.print("\nMalevich Space configuration [green]successfully[/green]"
               " added to the manifest\n")

    return True
