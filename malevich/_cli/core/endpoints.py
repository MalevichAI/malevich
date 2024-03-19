import rich
from typer import Typer

from malevich.core_api import (
    delete_endpoint,
    get_endpoint,
    get_endpoints,
    get_run_endpoint,
    pause_endpoint,
    resume_endpoint,
)

from ..misc.make import wrap_command

endpoints_app = Typer(name='endpoints')


@endpoints_app.command(name='list')
@wrap_command(get_endpoints)
def list_endpoints(verbose: bool = False, **kwargs) -> None:
    endpoints = get_endpoints(**kwargs)
    if not verbose:
        rich.print(
            '\n'.join([f'{e.hash} {e.description}' for e in endpoints.data]))
    else:
        rich.print(endpoints.data)


@endpoints_app.command(name='get')
@wrap_command(get_endpoint)
def get(**kwargs) -> None:
    endpoint = get_endpoint(**kwargs)
    rich.print(endpoint)


@endpoints_app.command(name='pause')
@wrap_command(pause_endpoint)
def pause(**kwargs) -> None:
    pause_endpoint(**kwargs)
    rich.print('Endpoint paused')


@endpoints_app.command(name='resume')
@wrap_command(resume_endpoint)
def resume(**kwargs) -> None:
    resume_endpoint(**kwargs)
    rich.print('Endpoint resumed')


@endpoints_app.command(name='delete')
@wrap_command(delete_endpoint)
def delete(**kwargs) -> None:
    delete_endpoint(**kwargs)
    rich.print('Endpoint deleted')


@endpoints_app.command(name='run-options')
@wrap_command(get_run_endpoint)
def run_options(**kwargs) -> None:
    run = get_run_endpoint(**kwargs)
    rich.print(run)
