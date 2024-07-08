import rich
from typer import Typer

from malevich.core_api import (
    get_user_limits,
    update_user_limits,
)

from ..misc.make import wrap_command

limits_app = Typer(name='limits')


@limits_app.command(name='my')
@wrap_command(get_user_limits)
def list_endpoints(**kwargs) -> None:
    from malevich.core_api import Config
    rich.print("Limits for user: ", Config.CORE_USERNAME)
    lims = get_user_limits(**kwargs)
    rich.print(lims.model_dump())


@limits_app.command(name='set')
@wrap_command(update_user_limits)
def get(**kwargs) -> None:
    update_user_limits(**kwargs)
    return get_user_limits()
