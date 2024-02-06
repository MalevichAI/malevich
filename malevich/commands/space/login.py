import re
from typing import Optional

import rich
from malevich_space.ops import SpaceOps
from malevich_space.schema import HostSchema, SpaceSetup
from rich.prompt import Prompt

from ...commands.prefs import prefs as prefs
from ...constants import DEFAULT_CORE_HOST, PROD_SPACE_API_URL
from ..manifest import ManifestManager


def login(
    api_url: str = PROD_SPACE_API_URL,
    core_url: str = DEFAULT_CORE_HOST,
    space_url: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> None:
    if not space_url:
        domain = re.search(r"\/\/(.*)api\.(.+)\/?", api_url)
        left = domain.group(1) if domain.group(1) else ''
        right = '.' + domain.group(2) if domain.group(2) else ''
        space_url = f'https://{left}space{right}/'
        base_space_url = f'{left}space{right}'.rstrip('/')
    else:
        domain = re.search(r"\/\/(.*)space\.(.+)\/?", space_url)
        left = domain.group(1) if domain.group(1) else ''
        right = '.' + domain.group(2) if domain.group(2) else ''
        base_space_url = f'{left}space{right}'.rstrip('/')
        api_url = f'https://{left}api{right}/'

    manf = ManifestManager()
    rich.print("[b]Welcome to [purple]Malevich Space[/purple]![/b]"
               " The command allows you to connect your account "
               f"to [bright_cyan]{space_url}[/bright_cyan]"
               "[bright_black]\nIf you don't have an account, "
               "please create one and come back.[/bright_black]\n"
               )

    if not username:
        username = Prompt.ask(
            f"E-mail (or Username) on [bright_cyan]{base_space_url}[/bright_cyan]")
    if not password:
        password = Prompt.ask("Password", password=True)

    setup = SpaceSetup(
        api_url=api_url,
        username=username,
        password=password,
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
        exit(-1)

    space_password = manf.put_secret("space_password", setup.password)
    setup.password = space_password
    manf.put("space", value=setup)
    rich.print("\nMalevich Space configuration [green]successfully[/green]"
               " added to the manifest\n")
