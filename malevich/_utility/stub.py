import importlib
import json
import os
import re
import tempfile
import typing
from hashlib import sha256
from pathlib import Path
from typing import Type

import pydantic_yaml as pydml
from datamodel_code_generator import generate
from deepdiff import DeepDiff
from jinja2 import Environment, FileSystemLoader
from pydantic import BaseModel, Field

import malevich
from malevich.constants import reserved_config_fields
from malevich.core_api import AppFunctionsInfo, ConditionFunctionInfo
from malevich.models.dependency import Dependency

from ..path import Paths


class Templates:
    """Templates for creating a package"""

    module_general_description = """\"\"\"
Third-party Malevich app. Use it in flow by importing any processor
from this module and calling it within @flow decorated function.
\"\"\"
"""

    module_specific_description = """\"\"\"
{description}

---

Third-party Malevich app. Use it in flow by importing any processor
from this module and calling it within @flow decorated function.
\"\"\"
"""

    imports = """
import typing
from typing import *

import malevich.annotations
from malevich.models import ConfigArgument
from malevich._meta.decor import proc
from malevich._utility import Registry
from malevich.models.nodes import OperationNode
from .scheme import *
"""

    registry = """
Registry().register("{operation_id}", {registry_record})
"""

    processor = """
{definition}

{processor_name} = proc(use_sinktrace={use_sinktrace}, config_model={config_model})(__{processor_name})
\"""{docstrings}\"""
"""  # noqa: E501

def create_package_stub(
    package_name: str,
    metascript: str,
    dependency: Dependency
) -> tuple[bool, str]:
    """
    Create a package with the given name and contents
    Args:
        package (str): The name of the package to create.
        metascript (str): The contents of the package.

    Returns:
        A tuple containing a boolean indicating whether the package was
        created successfully, and the checksum of the package.
    """
    # Calculate the checksum of the package contents
    checksum = sha256(metascript.encode()).hexdigest()

    # Create the package directory
    # The package will be located at <python>/malevich/<package>
    package_path = Path(malevich.__file__).parent / package_name
    # Create the package directory if it doesn't exist
    os.makedirs(package_path, exist_ok=True)

    # Create the package __init__.py file
    with open(package_path / "__init__.py", "w") as f:
        # Write the metascript to the file
        f.write(metascript)
        # Write the checksum to the file
        f.write(f"\n__Metascript_checksum__ = '{checksum}'\n")
        f.write(f"\n__Dependency_checksum__ = '{dependency.checksum()}'\n")

    # Post-hoc check to make sure the package was created successfully
    try:
        # Import the package
        __pkg = importlib.import_module(f"malevich.{package_name}")
        # Assert that the checksum of the package is the same as the checksum
        assert getattr(__pkg, "__Metascript_checksum__") == checksum, \
            "Package checksum does not match metascript checksum. "
    except (ImportError, AssertionError):
        return False, checksum
    return True, checksum


class StubFunction(BaseModel):
    name: str
    # (name, type,)
    args: list[tuple[str, str | None]]
    # (name, pos,)
    sink: tuple[str, int] | None
    docstrings: str | None = None
    config_schema: Type[BaseModel] | None = Field(None, exclude=True)
    definition: str | None = None
    package_id: str
    is_condition: bool = False
    operation_id: str

    def generate_definition(self, config_model: str | None = None) -> str:
        """def processor_name(
            arg1: type1,
            arg2: type2,
            *,
            var1: type1,
            var2: type2 | None = None
            config: ConfigModel,
            **kwargs
        ) -> RETURN_TYPE:
        """
        environment = Environment(
            loader=FileSystemLoader(Paths.templates('metascript', 'op_stub'))
        )
        extra_kwargs_ = {}
        if self.config_schema:
            config_fields = self.config_schema.model_fields
            for field in config_fields:
                if field == "config":
                    raise ValueError(
                        "Trying to generate a definition for "
                        "a function with a config field named 'config'"
                    )
                annotation = config_fields[field].annotation
                if (
                    hasattr(annotation, "__name__")
                    and annotation.__module__ != typing.__name__
                ):
                    # class
                    annotation = config_fields[field].annotation.__name__
                else:
                    # etc.
                    annotation = str(config_fields[field].annotation)
                config_fields[field].annotation = annotation

            extra_kwargs_['config_fields'] = config_fields
        if self.sink:
            extra_kwargs_['sink'] = self.sink

        data = environment.get_template('def.jinja2').render(
            processor_name = self.name,
            args = self.args,
            docstrings = self.docstrings,
            is_condition = self.is_condition,
            package_id = self.package_id,
            config_model=config_model,
            reserved_config_fields=reserved_config_fields,
            operation_id=self.operation_id,
            **extra_kwargs_
        )

        return data


class StubSchema(BaseModel):
    name: str
    scheme: str
    class_name: str


class StubIndex(BaseModel):
    dependency: Dependency
    functions: list[StubFunction]
    functions_index: dict[str, tuple[int, int]]
    schemes: list[StubSchema]
    schemes_index: dict[str, tuple[int, int]]


class Stub:
    class Utils:
        @staticmethod
        def combine_schemas(schemas: list[str]) -> str:
            schemas = [json.loads(schema) for schema in schemas]
            all_defs = {}
            for schema in schemas:
                if not schema:
                    continue

                defs = schema.get('$defs', schema.get('definitions', {}))
                for def_name, def_schema in defs.items():
                    if def_name in all_defs:
                        diff = DeepDiff(all_defs[def_name], def_schema)
                        if diff:
                            raise ValueError(
                                "Duplicate schemas found with different content: "
                                f"{def_name}"
                            )
                    else:
                        all_defs[def_name] = def_schema

                if 'title' in schema:
                    all_defs[schema['title']] = schema

            return json.dumps({
                "$defs": all_defs,
                "type": "object",
            })


        @staticmethod
        def generate_context_schema(json_schema: str) -> tuple[str, str]:
            with (
                tempfile.NamedTemporaryFile(mode='w+', suffix='.json') as f,
                tempfile.NamedTemporaryFile(mode='w+', suffix='.py') as out
            ):
                f.write(json_schema)
                f.seek(0)
                generate(
                    Path(f.name),
                    output=Path(out.name),
                    use_annotated=False,
                    input_file_type='jsonschema',
                    base_class='malevich.models._model._Model', # cool
                    collapse_root_models=True,
                    disable_timestamp=True,
                    custom_file_header='"""Malevich auto-generated schema"""',
                )
                out_script =  open(out.name).read().replace(
                    'from __future__ import annotations',
                    ''
                )
                return (
                    re.findall(
                        r'class (?P<ClassName>\w+)',
                        out_script
                    ),   # 1
                    out_script              # 2
                )


    def __init__(
        self,
        path: str,
        malevich_app_name: str | None = None,
        dependency: Dependency | None = None
    ) -> None:
        ipath = os.path.join(path, 'index.yaml')
        assert Path(ipath).exists(), "No index.yaml found in the package"
        self.index = pydml.parse_yaml_file_as(StubIndex, ipath)
        self.path = path
        self.malevich_app_name = malevich_app_name
        self.dependency = dependency

    @staticmethod
    def from_app_info(
        path: str,
        package_name: str,
        dependency: Dependency,
        app_info: AppFunctionsInfo,
        operation_ids: dict[str, str],
        registry_records: dict[str, dict],
        description: str | None = None,
    ) -> "Stub":
        os.makedirs(path, exist_ok=True)
        operations = {**app_info.processors, **app_info.conditions}
        operations = {str(key): value for key, value in operations.items()}

        index = StubIndex(
            dependency=dependency,
            functions=[],
            functions_index={},
            schemes=[],
            schemes_index={},
        )
        config_stubs = {
            name: Stub.Utils.generate_context_schema(
                json.dumps(processor.contextClass),
            ) if processor.contextClass is not None else (None, None)
            for name, processor in operations.items()
        }

        schemes = [
            *app_info.schemes.values(),
            *[json.dumps(processor.contextClass) for processor in operations.values()]
        ]

        config_model_class = {}
        for processor_name, (class_names, stub) in config_stubs.items():
            if class_names:
                for class_name in class_names:
                    if class_name == operations[processor_name].contextClass['title']:
                        config_model_class[processor_name] = class_name
                        break
                else:
                    config_model_class[processor_name] = None
            else:
                config_model_class[processor_name] = None


        with open(os.path.join(path, 'scheme.py'), 'w+') as f_scheme:
            f_scheme.write(
                Stub.Utils.generate_context_schema(
                    Stub.Utils.combine_schemas(schemes)
                )[1]
            )


        importlib.import_module(f'malevich.{package_name}.scheme')
        config_models = {
            name: eval(f'malevich.{package_name}.scheme.{config_model_class[name]}')
            if config_model_class[name] else None
            for name in operations
        }

        functions: dict[str, StubFunction] = {}
        schemes = {*app_info.schemes.values()}

        for name, processor in operations.items():
            args = processor.arguments
            parsed = []
            sink = None

            for idx, (alias, type_) in enumerate(args):
                if 'return' in alias:
                    continue
                if type_:
                    if 'Sink' in type_:
                        sink = (alias, idx,)
                        continue
                    if 'Context' in type_:
                        continue
                    for stub in schemes:
                        if stub in type_:
                            type_ = stub
                            break
                parsed.append((
                    alias,
                    'malevich.annotations.OpResult | malevich.annotations.Collection'
                ))

            functions[name] = StubFunction(
                name=name,
                args=parsed,
                sink=sink,
                docstrings=processor.doc,
                config_schema=config_models[name],
                package_id=package_name,
                is_condition = isinstance(processor, ConditionFunctionInfo),
                operation_id=operation_ids[name]
            )
            functions[name].definition = functions[name].generate_definition(
                config_model=config_model_class[name],
            )

        with open(os.path.join(path, 'F.py'), 'w+') as f_F:  # noqa: N806
            i = f_F.write(Templates.imports)

            for name, function in functions.items():
                if name not in operation_ids:
                    continue

                j = f_F.write(Templates.registry.format(
                    operation_id=operation_ids[name],
                    registry_record=registry_records[name]
                ))

                j += f_F.write(Templates.processor.format(
                    use_sinktrace=bool(function.sink),
                    definition=function.definition,
                    config_model=config_model_class[name],
                    processor_name=function.name,
                    docstrings=function.docstrings
                ))
                index.functions.append(function)
                index.functions_index[name] = (i, i + j)
                i += j

        pydml.to_yaml_file(os.path.join(path, 'index.yaml'), index)
        with open(os.path.join(path, '__init__.py'), 'w+') as init:
            if description:
                init.write(Templates.module_specific_description.format(
                    description=description
                ))
            else:
                init.write(Templates.module_general_description)

            init.write(
                "\n"
                "from .F import *\n"
                "from .scheme import *\n"
            )

        return Stub(
            path=path,
            malevich_app_name=package_name,
            dependency=dependency
        )
