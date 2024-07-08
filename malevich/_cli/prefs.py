import typer

from malevich.manifest import ManifestManager
from malevich.models.preferences import UserPreferences

prefs = typer.Typer(
    help="Manage user preferences",
)

main_keys = UserPreferences.model_fields.keys()


@prefs.command("set", help="Set a preference")
def set_pref(
    key: str = typer.Argument(..., help="Preference key"),
    value: str = typer.Argument(..., help="Preference value"),
) -> None:
    manf = ManifestManager()
    prefs_ = manf.query("preferences")
    prefs = UserPreferences(**prefs_) if prefs_ else UserPreferences()

    keys = key.split(".")
    if keys[0] not in main_keys:
        typer.echo(f"Unknown preference key: {keys[0]}. Available keys are: {', '.join(main_keys)}")  # noqa: E501
        raise typer.Exit(1)

    if len(keys) == 1:
        prefs.__setattr__(keys[0], value)
    else:
        cursor = prefs
        for key in keys[:-1]:
            try:
                if isinstance(cursor, dict):
                    cursor = cursor.__getitem__(key)
                else:
                    cursor = cursor.__getattribute__(key)
            except AttributeError:
                typer.echo(f"Unknown preference subkey: {key} in {'.'.join(keys)} at position {keys.index(key)}")  # noqa: E501
                raise typer.Exit(1)
        if isinstance(cursor, dict):
            cursor.__setitem__(
                keys[-1], type(cursor.__getitem__(keys[-1]))(value))
        else:
            cursor.__setattr__(
                keys[-1], type(cursor.__getattribute__(keys[-1]))(value))
    manf.put("preferences", value=prefs.model_dump())
    typer.echo(f"Set preference {key} to {value}")


@prefs.command("get", help="Get a preference")
def get_pref(
    key: str = typer.Argument(..., help="Preference key"),
) -> None:
    manf = ManifestManager()
    prefs_ = manf.query("preferences")
    prefs = UserPreferences(**prefs_) if prefs_ else UserPreferences()

    keys = key.split(".")
    if keys[0] not in main_keys:
        typer.echo(
            f"Unknown preference key: {keys[0]}. "
            f"Available keys are: {', '.join(main_keys)}"
        )

    if len(keys) == 1:
        typer.echo(prefs.__getattribute__(keys[0]))

    else:
        cursor = prefs
        for key in keys:
            try:
                if isinstance(cursor, dict):
                    cursor = cursor.__getitem__(key)
                else:
                    cursor = cursor.__getattribute__(key)
            except AttributeError:
                typer.echo(f"Unknown preference subkey: {key} in {'.'.join(keys)} at position {keys.index(key)}")  # noqa: E501
                raise typer.Exit(1)

        typer.echo(cursor)
