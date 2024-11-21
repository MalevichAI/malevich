import importlib.util
import pickle

import typer

from malevich.interpreter import SpaceInterpreter
from malevich.manifest import ManifestManager
from malevich.models import FlowFunction, PromisedTask

manf = ManifestManager()

flow = typer.Typer(
    name='flow',
    help="Manage flows uploading them without running",
)


@flow.command("export", help="Upload a flow to Malevich Core")
def export_flow(
    python_file: str = typer.Argument(...,
                                      help="Path to a Python file with a flow"),
    prefix: str = typer.Option("", help="Prefix for the flow name"),
) -> None:
    # Importing python file with a flow
    spec = importlib.util.spec_from_file_location("flow", python_file)

    if spec is None:
        raise typer.Exit(1)

    flow = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(flow)

    fn = []
    # Exporting flow

    for obj in flow.__dict__.values():
        if isinstance(obj, FlowFunction):
            fn.append(obj)

    if len(fn) == 0:
        typer.echo(f"There is no flows in {python_file}. Create one with @flow decorator")  # noqa: E501

    for f in fn:
        try:
            task: PromisedTask = f()
        except Exception as e:
            typer.echo(f"Skipping {f.reverse_id}. Most probably you are lacking "
                       "some modules installed, or the function requires arguments. "
                       f"Error: {e}")
        task.tree

        # Uploading secrets
        # TODO: upload secrets

        # Dumping flow to json
        flow_data = {
            "tree": task.tree,
            "apps": [next(iter(app.keys())) for app in manf.query("dependencies")]
        }
        flow_bytes = pickle.dumps(flow_data)

        with open(f"{prefix}{f.reverse_id}.malevichflow", "wb") as fl:
            fl.write(flow_bytes)

        typer.echo(
            f"Flow {f.reverse_id} was exported to {prefix}{f.reverse_id}.malevichflow"
        )


@flow.command("upload", help="Upload a flow to Malevich Space")
def upload_flow(
    path: str = typer.Argument(
        ...,
        help="Path to a file with a flow"
    ),
) -> None:
    # Uploading flow
    with open(path, "rb") as fl:
        flow_bytes = fl.read()
    data = pickle.loads(flow_bytes)
    tree = data["tree"]
    apps = data["apps"]
    for app in apps:
        importlib.import_module(f"malevich.{app}")

    typer.echo('Tree is restored successfully. Trying to interpret'
               ' with default Space Interpreter')

    interpreter = SpaceInterpreter()
    interpreter.interpret(tree)
    typer.echo(f"Flow {tree.name} was uploaded")
