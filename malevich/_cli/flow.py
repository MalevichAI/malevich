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
        help='Flow Deployment ID. If not set, will get active version flow.'
    ),
    branch: str=typer.Option(
        None,
        '--branch',
        '-b',
        show_default=False,
        help="Flow branch. If not specified, will take the active one."
    ),
    attach_any: bool=typer.Option(
        False,
        '--attach_any',
        '-a',
        show_default=False,
        help="Attach to any flow deployment"
    )
):
    my_flows_path = my_flows.__file__
    attach_to_last = deployment_id is None
    task = Space(
        reverse_id=reverse_id,
        attach_to_any=attach_any,
        attach_to_last=attach_to_last,
        deployment_id=deployment_id,
        branch=branch
    )
    if task.get_stage().value == 'interpreted':
        task.prepare()
    version_name = task.component.version.readable_name
    flows = open(my_flows_path).read()
    if f"def {underscored(reverse_id)}(\n\tversion: Literal['{version_name}']" in flows:
        rich.print(
            f"The integration named [yellow]{underscored(reverse_id)}[/yellow] with "
            f"version [blue]{version_name}[/blue] already exists.\n"
            "You can run it with passing another [green]deployment_id[/green] "
        )
        exit(1)
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
            f"\tversion='{version_name}',\n"
            f"\tmapping={mappings}\n)\n"
            "@overload\n"
            f"def {underscored(reverse_id)}(\n"
            f"\tversion: Literal['{version_name}'],\n"
            f"{args_},\n"
            "\tdeployment_id=None,\n"
            "\tattach_to_last=True,\n"
            "\tattach_to_any=False\n) -> list[SpaceCollectionResult]:\n"
            "\t...\n"
        )
    rich.print(
        f"Successfully installed: [yellow]{reverse_id}[/yellow], "
        f"version [blue]{version_name}[/blue]"
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
    ),
    version: str=typer.Option(
        None,
        '--version',
        '-v',
        show_default=False,
        help="Version to delete."
    ),
    delete_all: bool=typer.Option(
        False,
        '--all-versions',
        '-a',
        show_default=False,
        help="Delete all versions of the flow."
    )
):
    if version is None and not delete_all:
        rich.print(
            "Either [violet]--version[/violet] or [violet]--all-versions[/violet] "
            "should be provided."
        )
        exit(1)
    my_flows_path = my_flows.__file__
    flows_ = open(my_flows_path).read()
    if delete_all:
        if underscored(reverse_id) not in flows_:
            rich.print(
                f"Flow named [yellow]{underscored(reverse_id)}[/yellow] was not found."
            )
            exit(1)
    else:
        version_reg = version
        for c in "\\.+*?^$()[]{}|":
            version_reg = version_reg.replace(c, f"\\{c}")
        if re.search(
            rf"def {underscored(reverse_id)}\(\s+?version: "
            rf"Literal\['{version_reg}'\]",
            flows_,
            flags=re.MULTILINE
        ) is None:
            rich.print(
                f"Flow [yellow]{underscored(reverse_id)}[/yellow] with version "
                f"[blue]{version}[/blue] was not found."
            )
            exit(1)

    if delete_all:
        version_reg = r"[\S\s]*?"
    else:
        version_reg = version
        for c in "\\.+*?^$()[]{}|":
            version_reg = version_reg.replace(c, f"\\{c}")

    with open(my_flows_path, 'w') as f:
        f.write(
            re.sub(
                rf"\n@installed_flow\(\s+reverse_id='{reverse_id}',\n\s+version='{version_reg}'"
                r"[\s\S]*?\)\n@overload\n"
                rf"def {underscored(reverse_id)}\(\n\s+version: "
                rf"Literal\['{version_reg}'\][\S\s]*?\.\.\.\n",
                '',
                flows_,
                flags=re.MULTILINE
            )
        )
        rich.print(
            f"{'All versions' if delete_all else 'version ' + version} "
            f"of [yellow]{reverse_id}[/yellow] was [green]successfully[/green] "
            f"deleted."
        )
