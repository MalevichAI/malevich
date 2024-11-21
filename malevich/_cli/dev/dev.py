import ast
import json
import os
import re
import shutil
import site
import sys
from pathlib import Path
from subprocess import CalledProcessError, call
from typing import Union

import rich
import typer
from datamodel_code_generator import InputFileType, generate
from typing_extensions import Annotated

dev = typer.Typer(help="Document operations")

SECTIONS = ["heading", "input", "output", "config", "example"]


def error_exit(msg: str = "") -> None:
    rich.print(f"[bold red]{msg}[/bold red]", file=sys.stderr)
    exit(1)


TYPES = {
    "str": "string",
    "int": "integer",
    "float": "number",
    "list": "array",
    "tuple": "array",
    "dict": "object",
    "bool": "boolean",
}


def parse_type(in_type: str) -> dict:
    result = []
    types = in_type.split("|")
    for t in types:
        if "[" in t:
            search = re.search(r"(?P<PARENT>.+?)\[(?P<CHILD>.+)\]", t)
            parent = search.group("PARENT")
            if parent in TYPES.keys():
                parent = TYPES[parent]
            child = search.group("CHILD")
            result.append({"type": parent, "items": {"anyOf": parse_type(child)}})

        else:
            if t in TYPES.keys():
                t = TYPES[t]
            result.append({"type": t})
    return result


def parse_in_out(input: str = "", idx: int = 0) -> list:
    result = []
    cases = input.split("---")
    for case in cases:
        case_json = {}
        if not re.search(r"^\s+\-", case, re.MULTILINE):
            case_json["columns"] = None
            case_json["summary"] = re.search(r"([\S\s]+)", case, re.MULTILINE).group(1)
        else:
            case_json["summary"] = re.search(
                r"([\S\s]+?)^\s+\-", case, re.MULTILINE
            ).group(1)
            case_json["columns"] = []
            columns = re.findall(
                r"^\s+\-\s(?P<COLUMN_NAME>`?\w+`?+)\s+\("
                r"(?P<COLUMN_TYPE>`?\w+`?)(?P<OPTIONAL_FLAG>,\soptional)?\)"
                r"\:[\s\n\t]+(?P<COLUMN_DESCRIPTION>.+)",
                case,
                flags=re.MULTILINE,
            )
            if len(columns) == 0:
                error_exit(f"Failed to parse {SECTIONS[idx]} columns")
            for column in columns:
                case_json["columns"].append(
                    {
                        "name": column[0],
                        "type": column[1],
                        "optional": True if column[2] != "" else False,
                        "description": column[3],
                    }
                )
        result.append(case_json)
    return result


def parse_config(config: str = "") -> list:
    result = []
    columns = re.findall(
        r"^\s+- (?P<FIELD_NAME>`?\w+`?)\:\s(?P<FIELD_TYPE>`?[|\w\[\{\<\]\}\>]+`?)"
        r"(?P<DEFAULT_CLAUSE>\,\sdefault (?P<DEFAULT_VALUE>.+))?\.[\s\n\t]+"
        r"(?P<FIELD_DESCRIPTION>.+)\.$",
        config,
        re.MULTILINE,
    )
    if len(columns) != len(re.findall(r"^\s+- ", config, re.MULTILINE)):
        in_doc = len(re.findall(r"^\s+- ", config, re.MULTILINE))
        error_exit(
            "Failed to parse config columns\n"
            f"Number of columns in doc (started with ' - ') is {in_doc}), "
            f"Number of columns parsed is {len(columns)}"
        )
    for column in columns:
        result.append(
            {
                "name": column[0],
                "type": column[1],
                "default": None if column[3] == "" else column[3],
                "description": column[4],
            }
        )
    return result


@dev.command("list-procs", help="List all processors and paths to their files")
def list_procs(
    path=typer.Argument(
        ".", help="Directory to extract processors from", show_default=False
    ),
    out: Annotated[
        Union[str, None],
        typer.Option("--out", help="File to save results in", show_default=False),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose", "-v", help="If enabled, will print result JSON to stdout"
        ),
    ] = False,
) -> str:
    data = []
    if os.path.isdir(path):
        for root, _, files in os.walk(path):
            for file in files:
                # Parsing python files
                if file.endswith(".py"):
                    with open(os.path.join(root, file)) as f:
                        # Parsing AST
                        tree = ast.parse(f.read())
                        for node in ast.walk(tree):
                            # Looking for functions with @processor decorator
                            if isinstance(node, ast.FunctionDef) or isinstance(
                                node, ast.AsyncFunctionDef
                            ):
                                function_name = node.name
                                decorators = [
                                    # id may be absent?
                                    d.func.id
                                    for d in node.decorator_list
                                    if isinstance(d, ast.Call)
                                ]
                                if "processor" in decorators:
                                    data.append(
                                        {
                                            "name": function_name,
                                            "path": os.path.abspath(f"{root}/{file}"),
                                        }
                                    )
    else:
        with open(path) as f:
            tree = ast.parse(f.read())
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef) or isinstance(
                    node, ast.AsyncFunctionDef
                ):
                    function_name = node.name
                    decorators = [
                        d.func.id
                        for d in node.decorator_list
                        if isinstance(d, ast.Call)
                    ]
                    if "processor" in decorators:
                        data.append(
                            {
                                "name": function_name,
                                "path": os.path.abspath(path),
                            }
                        )

    if out is not None:
        with open(out, "w") as f:
            f.write(json.dumps(data))
    if verbose:
        rich.print(json.dumps(data))
    return json.dumps(data)


@dev.command("get-doc", help="Get process docstring")
def get_processor_docstring(
    name=typer.Argument(
        ...,
        help="Processor name",
    ),
    path=typer.Argument(
        ...,
        help="Path to the file",
    ),
    out: Annotated[
        Union[str, None],
        typer.Option("--out", help="File to save results in", show_default=False),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose", "-v", help="If enabled, will print result docstring to stdout"
        ),
    ] = False,
) -> str:
    if not os.path.exists(path):
        error_exit(f"Can not open {path}. No such file exists")

    with open(path) as f:
        tree = ast.parse(f.read())
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.FunctionDef)
                or isinstance(node, ast.AsyncFunctionDef)
            ) and node.name == name:
                doc = ast.get_docstring(node)
                if out is not None:
                    open(out, "w").write(doc)
                if verbose:
                    rich.print(doc)
                return doc

    rich.print(
        "Could not find processor inside a file. "
        "Make sure you have provided correct arguments"
    )
    exit(1)


@dev.command("parse-doc", help="Parse processor docstring")
def parse_docstring(
    doc=typer.Argument(..., help="docstring"),
    file: Annotated[
        bool,
        typer.Option("--file", "-f", help="If enabled, will read from argument string"),
    ] = False,
    out: Annotated[
        Union[str, None],
        typer.Option("--out", help="File to save results in", show_default=False),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose", "-v", help="If enabled, will print result JSON to stdout"
        ),
    ] = False,
) -> str:
    idx = 0
    if file:
        doc = open(doc).read()

    if not doc:
        error_exit("Missing documentation")

    doc = doc.replace("\n", "\\n ")
    try:
        user = re.search(r"(?P<USER>[\S\s]+)-----(?P<DEV>[\S\s]*)", doc).group("USER")
    except Exception:
        error_exit("Failed to divide doc into User/Dev sections")

    result = {}

    try:
        heading = (
            re.search(r"(?P<HEADING>[\S\s]+?)(##|$)", user)
            .group("HEADING")
            .replace("\\n ", "\n")
        )
        result["heading"] = heading
        idx += 1

        input = (
            re.search(r"## Input:\\n(?P<INPUT>[\S\s]+?)(##|$)", user)
            .group("INPUT")
            .replace("\\n ", "\n")
        )
        result["input_section"] = parse_in_out(input, idx)
        idx += 1

        output = (
            re.search(r"## Output:\\n(?P<OUTPUT>[\S\s]+?)(#|$)", user)
            .group("OUTPUT")
            .replace("\\n ", "\n")
        )
        result["output_section"] = parse_in_out(output, idx)

        if "## Configuration" in doc:
            idx += 1
            config = (
                re.search(r"## Configuration:\\n(?P<CONFIG>[\S\s]+?)(#|$)", user)
                .group("CONFIG")
                .replace("\\n ", "\n")
            )
            result["configuration"] = parse_config(config)
        if "## Example" in doc:
            idx += 1
            example = (
                re.search(r"## Example:\\n(?P<EXAMPLE>[\S\s]+?)(#|$)", user)
                .group("EXAMPLE")
                .replace("\\n ", "\n")
            )
            result["example"] = example
    except Exception:
        error_exit(f"Docstring doesn't suit the rules in {SECTIONS[idx]} section")

    if out:
        open(out, "w").write(json.dumps(result))
    if verbose:
        rich.print(json.dumps(result))
    return json.dumps(result)


@dev.command("install-lib-hook", help="Configure git hooks path")
def intstall_hook(
    path=typer.Option(
        ".githooks/",
        help="Folder with hooks",
        show_default=False,
    ),
) -> None:
    if not os.path.exists(path):
        error_exit(
            f"Cannot find {path}. "
            "Make sure you've provided a valid path, and run from the right directory",
        )

    try:
        call(["git", "config", "core.hooksPath", path])
    except CalledProcessError:
        error_exit("Failed to execute the process")


@dev.command("make-configs", help="Create Context configurations for processors")
def make_config(
    path=typer.Argument(..., help="Path to start directory"),
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose", "-v", help="If enabled, will print result JSON to stdout"
        ),
    ] = False,
) -> None:
    data = json.loads(list_procs(path))
    by_path = {}
    for d in data:
        names: list = by_path.get(d["path"], [])
        names.append(d["name"])
        by_path[d["path"]] = names

    for path_ in by_path.keys():
        modules = []
        models_path = None
        rich.print(f"Processing {path_}...")
        for name_ in by_path[path_]:
            doc = get_processor_docstring(name_, path_)
            schema = json.loads(parse_docstring(doc))
            if "configuration" in schema.keys() and len(schema["configuration"]) > 0:
                schema_path = os.path.join(
                    os.path.dirname(path_), "models", f"{name_}_model.json"
                )
                schema_model = os.path.join(
                    os.path.dirname(path_), "models", f"{name_}_model.py"
                )
                models_path = os.path.dirname(schema_model)
                if not os.path.exists(os.path.dirname(schema_path)):
                    os.mkdir(os.path.dirname(schema_path))
                props = {}
                required = []
                for conf in schema["configuration"]:
                    conf_json = {}
                    conf_name = conf["name"].strip("`\"'")
                    conf_json["anyOf"] = parse_type(conf["type"])
                    if conf["default"] is not None:
                        conf_json["default"] = eval(conf["default"])
                    else:
                        required.append(conf_name)
                    if "description" in conf.keys():
                        conf_json["description"] = conf["description"]
                    props[conf_name] = conf_json

                schema = json.dumps(
                    {"type": "object", "properties": props, "required": required}
                )

                with open(schema_path, "w") as f:
                    f.write(schema)

                generate(
                    Path(schema_path),
                    output=Path(schema_model),
                    class_name=name_,
                    input_file_type=InputFileType.JsonSchema,
                    disable_timestamp=True
                )
                model = open(schema_model).read()
                model = re.sub(
                    r"^class", "@scheme()\nclass", model, flags=re.MULTILINE
                ).replace(
                    "from __future__ import annotations",
                    "from __future__ import annotations\n"
                    "from malevich.square import scheme",
                )

                with open(schema_model, "w") as f:
                    f.write(model)
                os.remove(schema_path)

                modules.append(
                    {
                        "name": name_,
                        "module": f"{name_}_model",
                        "classname": re.search(
                            r"^class (?P<CLASS>\w+)\(", model, flags=re.MULTILINE
                        ).group("CLASS"),
                    }
                )
        if models_path is not None:
            init_file = os.path.join(models_path, "__init__.py")
            Path(init_file).touch()
            file = open(init_file).read()
            procs = open(path_).read()
            for module in modules:
                search_import = re.search(
                    rf"^from .{module['module']} import {module['classname']}",
                    file,
                    flags=re.MULTILINE,
                )
                if search_import is None:
                    if verbose:
                        rich.print(f"Added {module['classname']} to __init__.py")
                    file += f"from .{module['module']} import {module['classname']}\n"
                if (
                    re.search(
                        rf"^from .models import {module['classname']}$",
                        procs,
                        flags=re.MULTILINE,
                    )
                    is None
                ):
                    if verbose:
                        rich.print(f"Added{module['classname']} to {path_}")
                    procs = f"from .models import {module['classname']}\n" + procs

                search_proc = re.search(
                    rf"(?P<DEF>^(async )?def {module['name']}"
                    r"\([\s\S]*? Context)(\[\w+\])?",
                    procs,
                    flags=re.MULTILINE,
                )
                group = search_proc.group("DEF").replace(
                    "Context", f"Context[{module['classname']}]"
                )
                procs = re.sub(
                    rf"(?P<DEF>^(async )?def {module['name']}"
                    r"\([\s\S]*? Context(\[\w+\])?)",
                    group,
                    procs,
                    flags=re.MULTILINE,
                )
            with open(init_file, "w") as f:
                f.write(file)
            with open(path_, "w") as f:
                f.write(procs)


@dev.command("procs-info", help="Get processors info in JSON format")
def procs_info(
    path=typer.Argument("./", show_default=False),
    out: Annotated[
        Union[str, None],
        typer.Option("--out", help="File to save results in", show_default=False),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option(
            ...,
            "--verbose",
            "-v",
            help="Verbose mode",
        ),
    ] = False,
) -> str | None:
    procs = json.loads(list_procs(path))
    if len(procs) == 0:
        rich.print("No procs found.")
        return
    info = []
    for p in procs:
        if verbose:
            rich.print(
                f"Processing [bold]{p['name']}[/bold] "
                f"from [italic]{p['path']}[/italic]"
            )
        doc = get_processor_docstring(p["name"], p["path"])
        info.append(json.loads(parse_docstring(doc)))

    if verbose:
        rich.print(info)
    if out is not None:
        with open(out, "w") as f:
            f.write(json.dumps(info))
    return json.dumps(info)


@dev.command("in-app-install", help="Install malevich inside the app")
def in_app_install() -> None:
    if os.getcwd() == "/julius":
        pkg_ = site.getsitepackages()[0]
        shutil.rmtree(os.path.join(pkg_, "malevich", "square"))
        shutil.copytree(
            "/julius/malevich/square", os.path.join(pkg_, "malevich", "square")
        )
        shutil.rmtree("/julius/malevich")
        shutil.move(os.path.join(pkg_, "malevich"), "/julius/malevich")
    else:
        error_exit("You are running command outside of the app")

@dev.command("get-regex", help="Get regexps by which we check documentation.")
def get_regexp() -> None:
    rich.print(
        "Input/Output columns:",
        "^\\s+\\-\\s(?P<COLUMN_NAME>`?\\w+`?+)\\s+\\("
        "(?P<COLUMN_TYPE>`?\\w+`?)(?P<OPTIONAL_FLAG>,\\soptional)?\\)"
        "\\:[\\s\\n\\t]+(?P<COLUMN_DESCRIPTION>.+)",
        "\n\n"
    )
    rich.print(
        "Config columns:",
        "^\\s+- (?P<FIELD_NAME>`?\\w+`?)\\:\\s"
        "(?P<FIELD_TYPE>`?[|\\w\\[\\{\\<\\]\\}\\>]+`?)"
        "(?P<DEFAULT_CLAUSE>\\,\\sdefault (?P<DEFAULT_VALUE>.+))?\\.[\\s\\n\\t]+"
        "(?P<FIELD_DESCRIPTION>.+)\\.$"
    )
