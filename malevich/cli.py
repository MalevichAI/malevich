import logging

import typer
import typer.core
from malevich_space.cli.cli import app as space_app

import malevich.help as help

from .commands.ci import app as ci_app
from .commands.flow import flow as flow_app
from .commands.install import auto_use
from .commands.list import list_packages
from .commands.manifest import app as manifest_app
from .commands.new import new
from .commands.prefs import prefs as prefs
from .commands.remove import remove
from .commands.restore import restore
from .commands.space.init import init
from .commands.space.login import login
from .commands.use import use as use_app
from .constants import APP_HELP

logging.getLogger("gql.transport.requests").setLevel(logging.ERROR)
app = typer.Typer(help=APP_HELP, rich_markup_mode="rich")

# =============== Register Commands ===============
# -------------------------------------------------

# Malevich Commands
# -------------------------------------------------

# malevich install
app.registered_commands.append(
    typer.models.CommandInfo(
        name="install",
        help=help.install["--help"],
        callback=auto_use,
        cls=typer.core.TyperCommand,
    )
)

# malevich restore
app.registered_commands.append(
    typer.models.CommandInfo(
        help=help.restore["--help"],
        callback=restore,
        cls=typer.core.TyperCommand
    )
)

# malevich remove
app.registered_commands.append(
    typer.models.CommandInfo(
        help=help.remove["--help"],
        callback=remove,
        cls=typer.core.TyperCommand
    )
)

# malevich list
app.registered_commands.append(
    typer.models.CommandInfo(
        name="list",
        help=help.list_["--help"],
        callback=list_packages,
        cls=typer.core.TyperCommand
    )
)

# malevich list
app.registered_commands.append(
    typer.models.CommandInfo(
        name="new",
        help=help.list_["--help"],
        callback=new,
        cls=typer.core.TyperCommand
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
# __________________________________________________


# =============== Register Groups ==================
# --------------------------------------------------


# malevich space flow
space_app.add_typer(flow_app, name="flow")

# malevich use
app.add_typer(use_app, name="use")

# malevich manifest
app.add_typer(manifest_app, name="manifest")

# malevich space
app.add_typer(space_app, name="space", help=help.space["--help"])

# malevich ci
app.add_typer(ci_app, name="ci", help=help.ci["--help"])

# malevich prefs
app.add_typer(prefs, name="prefs")

# _________________________________________________


def main() -> None:
    """Entry point"""
    app()


if __name__ == "__main__":
    main()
