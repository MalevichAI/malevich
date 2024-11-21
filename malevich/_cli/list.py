from typing import Annotated

import rich
import typer

from malevich._utility.package import package_manager
from malevich.manifest import manf
from malevich.models import Dependency


def list_packages(
    print_installers: Annotated[bool, typer.Option("--installers", "-i")] = False,
    print_versions: Annotated[bool, typer.Option("--versions", "-v")] = False,
) -> None:
    for package in package_manager.get_all_packages():
        try:
            dep_ = Dependency(**manf.query('dependencies', package))
        except Exception:
            continue

        message_ = f"{package}"
        if print_installers or print_versions:
            message_ += " [bright_black]("
        if print_installers:
            message_ += f"installer={dep_.installer}, "
        if print_versions:
            message_ += f"version={dep_.version}  "
        if print_installers or print_versions:
            message_ = message_[:-2] + ")[/bright_black]"

        rich.print(message_)
