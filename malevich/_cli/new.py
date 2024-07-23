import os
import shutil
from typing import Annotated

import rich
import typer

from malevich._utility import package_manager


def new(
    app_name: Annotated[str, typer.Argument(...)],
    base_path: Annotated[
        str, typer.Option("--path", "-p", show_default=False)
    ] = os.getcwd(),
    force: Annotated[bool, typer.Option("--force", "-f", show_default=False)] = False,
) -> None:
    dockerfile_ = os.path.join(
        package_manager.get_malevich_path(),
        "_templates",
        "Dockerfile.app"
    )

    if os.path.exists(os.path.join(base_path, app_name)) and not force:
        rich.print(
            f"[red]App {app_name} already exists. Use -f to overwrite[/red]"
        )
    elif os.path.exists(os.path.join(base_path, app_name)) and force:
        shutil.rmtree(
            os.path.join(base_path, app_name)
        )

    os.makedirs(
        os.path.join(
            base_path,
            app_name
        ),
        exist_ok=True
    )

    os.makedirs(
        os.path.join(
            base_path,
            app_name,
            "apps"
        ),
        exist_ok=True
    )

    shutil.copyfile(
        dockerfile_,
        os.path.join(
            base_path,
            app_name,
            "Dockerfile"
        )
    )

    with open(
        os.path.join(
            base_path,
            app_name,
            'apps',
            "processors.py"
        ),
        "w"
    ) as f:
        f.write(
            open(
                os.path.join(
                    package_manager.get_malevich_path(),
                    "_templates",
                    "processor.py.txt"
                )
            ).read()
        )

    with open(
        os.path.join(
            base_path,
            app_name,
            "flow.py"
        ),
        "w"
    ) as f:
        f.write(
            open(
                os.path.join(
                    package_manager.get_malevich_path(),
                    "_templates",
                    "flow.py.txt"
                )
            ).read().format(app=app_name)
        )

    with open(
        os.path.join(
            base_path,
            app_name,
            "requirements.txt"
        ),
        "w"
    ) as f:
        f.write("# Add your dependencies here\n")

    with open(
        os.path.join(
            base_path,
            app_name,
            "README.md"
        ),
        "w"
    ) as f:
        f.write(
            open(
                os.path.join(
                    package_manager.get_malevich_path(),
                    "_templates",
                    "README.app.md"
                )
            ).read().format(app=app_name)
        )
