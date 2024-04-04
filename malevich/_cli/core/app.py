from malevich_space.schema import SpaceSetup
from typer import Context, Option, Typer

from malevich.core_api import set_host_port, update_core_credentials

from ..._utility.space.get_core_creds import get_core_creds_from_setup
from ...constants import DEFAULT_CORE_HOST
from ...manifest import manf
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
    if user is None or password is None:
        try:
            user, password = get_core_creds_from_setup(
                SpaceSetup(**manf.query('space', resolve_secrets=True))
            )
        except Exception:
           raise Exception("You have not authorized. Run `malevich space login` first.")

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
    try:
        user, password = get_core_creds_from_setup(
            SpaceSetup(**manf.query('space', resolve_secrets=True))
        )
    except Exception:
        raise Exception("You have not authorized. Run `malevich space login` first.")

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
