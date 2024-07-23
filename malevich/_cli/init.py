import rich

from malevich.manifest import ManifestManager


def init():
    try:
        if ManifestManager().local:
            rich.print("\nProject is already initialized.")
            return
    except FileNotFoundError:
        pass

    ManifestManager.create_manifest()

    rich.print(
        "Initialized [purple]Malevich[/purple] project:\n"
        " - [light_black][i]malevich.yaml[/i] contains project settings[/light_black]\n"
        " - [light_black][i]malevich.secrets.yaml[/i] keeps project secrets[/light_black]"   # noqa: E501
    )
