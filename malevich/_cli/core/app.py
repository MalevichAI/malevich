from malevich_space.schema import SpaceSetup
from typer import Context, Option, Typer

from malevich._utility import get_core_creds_from_setup
from malevich._utility._try import try_cascade
from malevich.constants import DEFAULT_CORE_HOST
from malevich.core_api import Config, set_host_port, update_core_credentials
from malevich.manifest import manf

from ..space.login import login
from .assets import assets_app
from .endpoints import endpoints_app
from .limits import limits_app

core_app = Typer(name='core')

@core_app.callback()
def callback(
    ctx: Context,
    user: str = Option(None, '--user', '-u', help="Username on Malevich Core"),
    password: str = Option(None, '--password', '-p', help="Password on Malevich Core"),
    host: str = Option(DEFAULT_CORE_HOST, '--host', '-h', help="Host of Malevich Core"),
) -> None:
    def creds():
        nonlocal user, password
        return get_core_creds_from_setup(
            SpaceSetup(**manf.query('space', resolve_secrets=True))
        )

    if user is None or password is None:
        creds_, exc = try_cascade(
            creds,
            lambda *args: login(no_input=False),
        )

        if exc or not creds_:
            raise Exception(
                "Failed to authorize. Run `malevich space login`"
            ) from exc

        user, password = creds_

    ctx.obj = {
        'auth': (user, password),
        'conn_url': host.strip('/') + '/'
    }

    update_core_credentials(user, password)
    set_host_port(host)

@core_app.command(name='whoami')
def whoami(
    show_password: bool = Option(
        False, '--show-password', help='Whether to print passwordof the user'
    )
) -> None:
    """Prints the current user"""
    user, password = Config.CORE_USERNAME, Config.CORE_PASSWORD

    print(f'User: {user}')
    if show_password:
        print(f'Password: {password}')



core_app.add_typer(endpoints_app, name='endpoints', help="Manage endpoints")
core_app.add_typer(assets_app, name='assets', help="Manage binary files")
core_app.add_typer(
    limits_app,
    name='limits',
    help="Retrieve and set limits for cloud resource"
)
