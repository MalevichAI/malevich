import typer

from malevich.commands.use import use as use_app
from malevich.constants import APP_HELP

app = typer.Typer(help=APP_HELP, rich_markup_mode="rich")
app.add_typer(use_app, name="use")


@app.command()
def cache() -> None:
    # TODO: Implement cache command
    pass


def main() -> None:
    app()


if __name__ == "__main__":
    main()
