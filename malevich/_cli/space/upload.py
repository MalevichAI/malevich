import importlib.util
import os
from pathlib import Path
from typing import Annotated

import rich
import rich.progress
import typer
import sys

from malevich._deploy import Space
from malevich.models.flow_function import FlowFunction

def import_file(filepath):
    module_name = os.path.splitext(os.path.basename(filepath))[0]
    
    spec = importlib.util.spec_from_file_location(module_name, filepath)
    if spec is None:
        return None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    return module

def upload(
    source_file: Annotated[str, typer.Argument(..., help="Path to the source file"),],
    flow_reverse_id: Annotated[str, typer.Option(..., help="Flow reverse ID"),] = None,
    branch: Annotated[str, typer.Option(..., help="Branch name to upload to"),] = None,
) -> None:
    """Upload a flow to the Space"""

    # Check whether path is module definition or file
    module = import_file(source_file).__dict__
    if module is None:
        raise ValueError(f"Could not import {source_file}")
    # Importing the source file
    if flow_reverse_id:
        flows = [module[flow_reverse_id]]
    else:
        flows = [
            flow for name in module
            if isinstance(flow := module[name], FlowFunction)
        ]

    with rich.progress.Progress(rich.progress.SpinnerColumn()) as progress:
        task = progress.add_task("Uploading flows", total=len(flows))
        for flow in flows:
            if not isinstance(flow, FlowFunction):
                rich.print(f"Skipping {flow} as it is not a @flow function")
            else:
                Space(flow).upload(branch=branch)
            progress.update(task, advance=1)
            rich.print(f"Flow {flow.reverse_id} uploaded" + (" to branch " + branch if branch else ""))  # noqa: E501
    rich.print("Upload complete")

