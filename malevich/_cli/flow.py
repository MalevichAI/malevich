import importlib.util
import pickle
import re
from string import punctuation

import rich
import typer

from .. import my_flows
from .._deploy import Space
from ..interpreter.space import SpaceInterpreter
from ..manifest import ManifestManager
from ..models.flow_function import FlowFunction
from ..models.task.promised import PromisedTask

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


def underscored(entry: str):
    punct = punctuation + " "
    for p in punct:
        entry = entry.replace(p, '_')
    return entry

@flow.command("install", help="Install flow to my flows")
def install_flow(
    reverse_id: str=typer.Argument(
        ...,
        show_default=False,
        help="Space Flow Reverse ID"
    ),
    deployment_id: str=typer.Option(
        None,
        '--deployment-id',
        '-d',
        show_default=False,
        help='Flow Deployment ID. If not set, will get the last run flow deployment'
    )
):
    my_flows_path = my_flows.__file__
    flows_ = open(my_flows_path).read()
    if f"def {underscored(reverse_id)}(" in flows_:
        rich.print(
            f"The integration named [yellow]{underscored(reverse_id)}[/yellow] already "
            "exists.\nYou can run it with passing another [green]deployment_id[/green] "
        )
        exit(1)
    attach_to_last = deployment_id is None
    task = Space(
        reverse_id=reverse_id,
        force_attach=True,
        attach_to_last=attach_to_last,
        deployment_id=deployment_id
    )
    task_did = task.state.aux.task_id
    mappings = {}
    args_ = []
    for col in task.get_injectables():
        mappings[underscored(col.alias)] = col.alias
        args_.append("\t" + underscored(col.alias) + "=None")

    args_ = ',\n'.join(args_)
    with open(my_flows_path, 'a') as f:
        f.write(
            "\n@installed_flow(\n"
            f"\treverse_id='{reverse_id}',\n"
            f"\tmapping={mappings},\n"
            f"\tdeployment_id='{task_did}'\n)\n"
            f"def {underscored(reverse_id)}(\n"
            f"{args_},\n"
            f"\tdeployment_id='{task_did}',\n"
            "\tattach_to_last=False\n) -> list[SpaceCollectionResult]:\n"
            "\t...\n"
        )

@flow.command(
    name='delete',
    help='Delete flow integration.'
)
def delete_flow(
    reverse_id: str=typer.Argument(
        ...,
        show_default=False,
        help="Space Flow Reverse ID"
    )
):
    my_flows_path = my_flows.__file__
    flows_ = open(my_flows_path).read()
    if underscored(reverse_id) not in flows_:
        typer.echo(
            f"Integration named {underscored(reverse_id)} was "
            "not found."
        )
        exit(1)
    else:
        with open(my_flows_path, 'w') as f:
            f.write(
                re.sub(
                    rf"\n@installed_flow\(\s+reverse_id='{reverse_id}'[\s\S]*?\)\n"
                    rf"def {underscored(reverse_id)}\([\s\S]*?\.\.\.\n",
                    '',
                    flows_,
                    flags=re.MULTILINE
                )
            )
