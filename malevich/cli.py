import typer

from malevich.commands.manifest import app as manifest_app
from malevich.commands.use import use as use_app
from malevich.constants import APP_HELP

app = typer.Typer(help=APP_HELP, rich_markup_mode="rich")
app.add_typer(use_app, name="use")
app.add_typer(manifest_app, name="manifest")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
