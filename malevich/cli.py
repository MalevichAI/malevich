import typer

from malevich.commands.use import use as use_app

app = typer.Typer()
app.add_typer(use_app, name="use")


@app.command()
def cache() -> None:
    # TODO: Implement cache command
    pass


def main() -> None:
    app()


if __name__ == "__main__":
    main()
