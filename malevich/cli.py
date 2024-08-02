import logging

import typer
import typer.core
from malevich_space.cli.cli import app as space_app

import malevich.help as help

from ._cli.ci import app as ci_app
from ._cli.core.app import core_app
from ._cli.dev.dev import dev as dev_app
from ._cli.flow import flow as flow_app
from ._cli.init import init as init_
from ._cli.install import auto_use
from ._cli.list import list_packages
from ._cli.manifest import app as manifest_app
from ._cli.new import new
from ._cli.prefs import prefs as prefs
from ._cli.remove import remove
from ._cli.restore import restore
from ._cli.space.init import init
from ._cli.space.login import login
from ._cli.space.upload import upload
from ._cli.space.whoami import get_user_on_space
from ._cli.use import use as use_app
from .constants import APP_HELP

logging.getLogger("gql.transport.requests").setLevel(logging.ERROR)
app = typer.Typer(
    help=APP_HELP,
    rich_markup_mode="rich",
)

# =============== Register Commands ===============
# -------------------------------------------------

# Malevich Commands
# -------------------------------------------------

groups = {
    'dm': 'Dependency Management',
    'pm': 'Project Management',
    'space': 'Space Management',
    'core': 'Engine (Core) API',
    'cfg': 'User Settings'
}

# malevich install
app.registered_commands.append(
    typer.models.CommandInfo(
        name="install",
        help=help.install["--help"],
        callback=auto_use,
        cls=typer.core.TyperCommand,
        rich_help_panel=groups['dm']
    )
)

# malevich restore
app.registered_commands.append(
    typer.models.CommandInfo(
        help=help.restore["--help"],
        callback=restore,
        cls=typer.core.TyperCommand,
        rich_help_panel=groups['dm']
    )
)

# malevich remove
app.registered_commands.append(
    typer.models.CommandInfo(
        help=help.remove["--help"],
        callback=remove,
        cls=typer.core.TyperCommand,
        rich_help_panel=groups['dm']
    )
)

# malevich list
app.registered_commands.append(
    typer.models.CommandInfo(
        name="list",
        help=help.list_["--help"],
        callback=list_packages,
        cls=typer.core.TyperCommand,
        rich_help_panel=groups['dm']
    )
)

# malevich new
app.registered_commands.append(
    typer.models.CommandInfo(
        name="new",
        help=help.new["--help"],
        callback=new,
        cls=typer.core.TyperCommand,
        rich_help_panel=groups['pm']
    )
)

# malevich init
app.registered_commands.append(
    typer.models.CommandInfo(
        name="init",
        help="Initializes new Malevich project",
        callback=init_,
        cls=typer.core.TyperCommand,
        rich_help_panel=groups['pm']
    )
)

# _________________________________________________

# Space Additional Commands
# -------------------------------------------------

# malevich space init
space_app.registered_commands.append(
    typer.models.CommandInfo(
        help=help.space["init --help"],
        callback=init,
        cls=typer.core.TyperCommand
    )
)

# malevich space login
space_app.registered_commands.append(
    typer.models.CommandInfo(
        help=help.space["login --help"],
        callback=login,
        cls=typer.core.TyperCommand
    )
)

space_app.registered_commands.append(
    typer.models.CommandInfo(
        "whoami",
        help=help.space["whoami --help"],
        callback=get_user_on_space,
        cls=typer.core.TyperCommand
    )
)


space_app.registered_commands.append(
    typer.models.CommandInfo(
        "upload-flow",
        help=help.space["upload-flow --help"],
        callback=upload,
        cls=typer.core.TyperCommand
    )
)
# __________________________________________________


# =============== Register Groups ==================
# --------------------------------------------------


# malevich space flow
space_app.add_typer(flow_app, name="flow")

# malevich use
app.add_typer(use_app, name="use", rich_help_panel=groups['dm'])

# malevich manifest
app.add_typer(manifest_app, name="manifest", rich_help_panel=groups["pm"])

# malevich space
app.add_typer(
    space_app,
    name="space",
    help=help.space["--help"],
    rich_help_panel=groups['space']
)

# malevich ci
app.add_typer(
    ci_app,
    name="ci",
    help=help.ci["--help"],
    rich_help_panel=groups['pm']
)

# malevich prefs
app.add_typer(prefs, name="prefs", rich_help_panel=groups['cfg'])

# malevich dev
app.add_typer(dev_app, name='dev', hidden=True)

#malevich core
app.add_typer(
    core_app,
    name='core',
    help=help.core['--help'],
    rich_help_panel=groups['core']
)
# _________________________________________________


class CLIContext:
    global_ = False

@app.callback()
def main_callback(
    ctx: typer.Context,
    global_: bool = typer.Option(
        False,
        "--global",
        "-g",
        help="Run the command in global context",
    ),
) -> None:
    ctx.obj = CLIContext()
    ctx.obj.global_ = global_

def main() -> None:
    """Entry point"""
    from malevich._cli.misc.manifest_to_env import manifest_as_env
    with manifest_as_env:
        app()


if __name__ == "__main__":
    main()
