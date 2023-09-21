def main():
    import typer
    from malevich.commands import use
    
    app = typer.Typer()
    app.add_typer(use.app, name="use")
    app()
