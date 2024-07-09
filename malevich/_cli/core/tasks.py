import rich
from malevich_coretools import get_run_active_runs, task_stop
from typer import Typer

tasks_app = Typer(name='task')

@tasks_app.command(name='clear', help='Clear all tasks on core')
def clear()-> None:
    for t in get_run_active_runs().ids:
        try:
            task_stop(t)
        except Exception:
            rich.print(
                f'Failed to stop task {t}. Most likely it has already been stopped.'
            )
