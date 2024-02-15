import ast
import json
import os
import re
import sys
from subprocess import CalledProcessError, call

import typer
from typing_extensions import Annotated

dev = typer.Typer(help="Document operations")

SECTIONS = ['heading', 'input', 'output', 'config', 'example']

def error_exit(msg: str=""):  # noqa: ANN201
    print(msg, file=sys.stderr)
    exit(1)

def parse_in_out(input: str="", idx:int=0) -> list:
    result = []
    cases = input.split('---')
    for case in cases:
        case_json = {}
        if not re.search(r"^\s+\-", case, re.MULTILINE):
            case_json['columns'] = None
            case_json['summary'] = re.search(r"([\S\s]+)", case, re.MULTILINE).group(1)
        else:
            case_json['summary'] = re.search(
                r"([\S\s]+?)^\s+\-", case, re.MULTILINE
            ).group(1)
            case_json['columns'] = []
            columns = re.findall(
                r"^\s+\-\s(?P<COLUMN_NAME>`?\w+`?+)\s+\("
                r"(?P<COLUMN_TYPE>`?\w+`?)(?P<OPTIONAL_FLAG>,\soptional)?\)"
                r"\:[\s\n\t]+(?P<COLUMN_DESCRIPTION>.+)",
                case,
                flags=re.MULTILINE
            )
            if len(columns) == 0:
                error_exit(f"Failed to parse {SECTIONS[idx]} columns")
            for column in columns:
                case_json['columns'].append(
                    {
                        'name': column[0],
                        'type': column[1],
                        'optional': True if column[2] != '' else False,
                        'description': column[3]
                    }
                )
        result.append(case_json)
    return result

def parse_config(config: str="") -> list:
    result = []
    columns = re.findall(
        r"^\s+- (?P<FIELD_NAME>`?\w+`?)\:\s"
        r"(?P<FIELD_TYPE>`?[\w\[\(\{\<\]\)\}\>]+`?)"
        r"(?P<DEFAULT_CLAUSE>\,\sdefault (?P<DEFAULT_VALUE>.+))?\."
        r"[\s\n\t]+(?P<FIELD_DESCRIPTION>.+)\.$",
        config,
        re.MULTILINE
    )
    if len(columns) == 0:
        error_exit("Failed to parse config columns")
    for column in columns:
        result.append(
            {
                'name': column[0],
                'type': column[1],
                'default': None if column[3] == "" else column[3],
                'description': column[4]
            }
        )
    return result


@dev.command('list-procs',help="List all processors and paths to their files")
def list_procs(  # noqa: ANN201
    path = typer.Argument(
        '.',
        help="Directory to extract processors from",
        show_default=False
    ),
    out = typer.Option(
        None,
        help="File to save results in",
        show_default=False
    )
):
    data = []

    for root, _, files in os.walk(path):
        for file in files:
            # Parsing python files
            if file.endswith('.py'):
                with open(os.path.join(root, file)) as f:
                    # Parsing AST
                    tree = ast.parse(f.read())
                    for node in ast.walk(tree):
                        # Looking for functions with @processor decorator
                        if isinstance(node, ast.FunctionDef):
                            function_name = node.name
                            decorators = [
                                # id may be absent?
                                d.func.id for d in node.decorator_list
                                if isinstance(d, ast.Call)
                            ]
                            if 'processor' in decorators:
                                data.append(
                                    {
                                        'name': function_name,
                                        'path': os.path.abspath(f'{root}/{file}'),
                                    }
                                )
    if out is not None:
        with open(out, 'w') as f:
            f.write(json.dumps(data))
    else:
        print(json.dumps(data))


@dev.command("get-doc",help="Get process docstring")
def get_processor_docstring(  # noqa: ANN201
    name = typer.Argument(
        ...,
        help="Processor name",
    ),
    path = typer.Argument(
        ...,
        help="Path to the file",
    )
):
    if not os.path.exists(path):
        error_exit(f"Can not open {path}. No such file exists")

    with open(path) as f:
        tree = ast.parse(f.read())
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == name:
                doc = ast.get_docstring(node)
                print(doc)
                exit(0)
    print(
        'Could not find processor inside a file. '
        'Make sure you have provided correct arguments'
    )
    exit(1)

@dev.command("parse-doc", help="Parse processor docstring")
def parse_docstring(  # noqa: ANN201
    doc = typer.Argument(
        ...,
        help="docstring"
    ),
    file: Annotated[
        bool,
        typer.Option(
            "--file",
            "-f",
            help="If enabled, will read from argument path")
    ] = False,
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            "-v",
            help="If enabled, will print result JSON to stdout"),
    ] = False,
):
    idx = 0
    if file:
        doc = open(doc).read()

    doc = doc.replace("\n", "\\n ")
    try:
        user = re.search(r"(?P<USER>[\S\s]+)-----(?P<DEV>[\S\s]*)", doc).group("USER")
    except Exception:
        error_exit("Failed to divide doc into User/Dev sections")

    result = {}

    try:
        heading = re.search(
            r"(?P<HEADING>[\S\s]+?)(##|$)",
            user
            ).group("HEADING").replace("\\n ", "\n")
        result['heading'] = heading
        idx += 1

        input = re.search(
            r"## Input:\\n(?P<INPUT>[\S\s]+?)(##|$)",
            user
            ).group("INPUT").replace("\\n ", "\n")
        result['input_section'] = parse_in_out(input, idx)
        idx += 1

        output = re.search(
            r"## Output:\\n(?P<OUTPUT>[\S\s]+?)(#|$)",
            user
            ).group("OUTPUT").replace("\\n ", "\n")
        result['output_section'] = parse_in_out(output, idx)

        if "## Configuration" in doc:
            idx += 1
            config = re.search(
                r"## Configuration:\\n(?P<CONFIG>[\S\s]+?)(#|$)",
                user
                ).group("CONFIG").replace("\\n ", "\n")
            result['configuration'] = parse_config(config)
        if "## Example" in doc:
            idx += 1
            example = re.search(
                r"## Example:\\n(?P<EXAMPLE>[\S\s]+?)(#|$)",
                user
                ).group("EXAMPLE").replace("\\n ", "\n")
            result['example'] = example
    except Exception:
        error_exit(f"Docstring doesn't suit the rules in {SECTIONS[idx]} section")

    if verbose:
        print(json.dumps(result))

@dev.command('install-lib-hook', help="Configure git hooks path")
def intstall_hook(  # noqa: ANN201
    path=typer.Option(
        ".githooks/",
        help="Folder with hooks"
    )
):
    if not os.path.exists(path):
        error_exit(
            f"Cannot find {path}. "
            "Make sure you've provided a valid path, and run from the right directory",
        )

    try:
        call(
            ["git", "config", "core.hooksPath", path]
        )
    except CalledProcessError:
        error_exit(
            "Failed to execute the process",
            file=sys.stderr
        )
