# import typer

# from ..manifest import ManifestManager
# from ..models.preferences import UserPreferences

# prefs = typer.Typer(
#     help="Manage user preferences",
# )

# main_keys = UserPreferences.model_fields.keys()

# @prefs.command("set", help="Set a preference")
# def set_pref(
#     key: str = typer.Argument(..., help="Preference key"),
#     value: str = typer.Argument(..., help="Preference value"),
# ):
#     manf = ManifestManager()
#     prefs_ = manf.query("preferences")
#     prefs = UserPreferences(**prefs_) if prefs_ else UserPreferences()

#     keys = key.split(".")
#     if keys[0] not in main_keys:
#         typer.echo(f"Unknown preference key: {keys[0]}. Available keys are: {', '.join(main_keys)}")  # noqa: E501
#         raise typer.Exit(1)

#     if len(keys) == 1:

