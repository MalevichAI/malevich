
import rich
import typer
from rich.prompt import Prompt
from rich.table import Table

from .._utility.dicts import flatdict
from ..manifest import ManifestManager

app = typer.Typer(name="manifest")
secrets = typer.Typer(name="secrets")


DISCLAIMER = """
[bold red]DISCLAIMER[/bold red]
[center]
\tNot manifested internal secrets are candidates for being removed without notice.
\tTo manifest a secret, add it to the [b]'secrets'[/b] section of the [yellow on black]manifest.secrets.yaml[/yellow on black] file.
\tAdditonally, you can use the [yellow on black]malevich secrets fix[/yellow on black] command to interactively manifest secrets.
[/center]
""" # noqa: E501


@secrets.command("scan")
def scan(
    only_number: bool = typer.Option(
        False,
        "--only-number",
        "-n",
        help="Only print the number of secrets found",
    ),
) -> None:
    manf = ManifestManager()
    flat = flatdict(manf.as_dict())
    manifested = manf.get_secrets()['secrets']

    secrets = [x for x in flat if x[1] and ManifestManager.is_secret(x[1])]

    table = Table(title="Secrets", box=rich.box.HORIZONTALS)
    table.add_column("Secret", justify='right')
    table.add_column("Path")
    table.add_column("Manifested", justify='center')

    rich.print(
        f"\n\n[white]Found [spring_green1]{len(secrets)}[/spring_green1] "
        "secrets in manifest[/white]"
    )
    rich.print("\n")
    should_print_legend = False
    for secret in secrets:
        sec = secret[1]
        if only_number:
            sec = sec.split("#")[1]
        tick = "[b green]✔[/b green]" if secret[1] in manifested else "[b red]✘[/b red]"
        if secret[1] in manifested and manifested[secret[1]]['external']:
            tick = '[b yellow]⚠[/b yellow]'
            should_print_legend = True
        table.add_row(sec, ".".join(map(str, secret[0])), tick)
    rich.print(table)
    if should_print_legend:
        rich.print("[bold yellow]⚠[/bold yellow] - external secret")
    rich.print("\n")

    if len(secrets) - len(manifested) > 0:
        rich.print(DISCLAIMER)


@secrets.command("restore")
def fix() -> None:
    manf = ManifestManager()
    flat = flatdict(manf.as_dict())
    secrets = [x for x in flat if x[1] and ManifestManager.is_secret(x[1])]
    manifested = manf.get_secrets()['secrets']
    not_manifested = [x for x in secrets if x[1] not in manifested]

    rich.print(f"\n\n[white]Found [spring_green1]{len(not_manifested)}[/spring_green1] "
               "secrets in manifest[/white]")
    rich.print("\n")

    for secret in not_manifested:
        rich.print(f'\tSecret: {secret[1]}')
        rich.print(f'\tUsed as: {".".join(map(str, secret[0]))}')
        rich.print("\n")
        key = Prompt.ask("Secret key", default=secret[0][-1])
        value = Prompt.ask("Secret value")
        mark_external = Prompt.ask("Mark as external? (y/n)") == "y"
        secret_key = manf.put_secret(key, value, external=mark_external)
        manf.put(*secret[0], value=secret_key, append=False)
        manf.save()
        rich.print(f"\n[green]Secret updated: [/green] {secret[1]} -> {secret_key}\n")


@app.command("query")
def query(path: list[str], resolve_secrets: bool = False) -> None:
    __q = ManifestManager().query(*path, resolve_secrets=resolve_secrets)
    if __q is None:
        rich.print("[red]Not found[/red]")
    else:
        rich.print(__q)


app.add_typer(secrets, name="secrets")
