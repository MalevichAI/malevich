import importlib
import json
import os
import re
import tempfile
from contextlib import chdir
from hashlib import sha256
from pathlib import Path
from typing import Type

import pydantic_yaml as pydml
from datamodel_code_generator import generate
from pydantic import BaseModel, Field

import malevich
from malevich.core_api import AppFunctionsInfo

from ..constants import reserved_config_fields
from ..models.dependency import Dependency


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
from typing import Any, Optional

import malevich.annotations
from malevich._meta.decor import proc
from malevich._utility.registry import Registry
from malevich.models.nodes import OperationNode
from .scheme import *
"""

    registry = """
Registry().register("{operation_id}", {registry_record})
"""

    processor = """
@proc(use_sinktrace={use_sinktrace}, config_model={config_model})
{definition}
    return OperationNode(
        operation_id="{operation_id}",
        config=config,
        processor_id="{name}",
        package_id="{package_id}",
        alias=alias,
    )
"""


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
        # def processor_name(
        def_ = "def " + self.name + "("
        for arg in self.args:
            # def processor_name(
            #    ...
            #    argX: typeX,
            def_ += f'\n\t{arg[0]}: {arg[1] or "Any"}' + ","
        # def processor_name(
        #    ...
        #    argX: typeX,
        #    *,
        if self.sink:
            def_ += f'\n\t*{self.sink[0]}: Any,'
        else:
            def_ += "\n\t*, "
        if self.config_schema is not None:
            config_fields = self.config_schema.model_fields
            for field in config_fields:
                if field == "config":
                    raise ValueError(
                        "Trying to generate a definition for "
                        "a function with a config field named 'config'"
                    )

                if 'typing' in str(config_fields[field].annotation):
                    # typing.Union, typing.Optional
                    annotation = str(config_fields[field].annotation)
                elif hasattr(config_fields[field].annotation, "__name__"):
                    # class
                    annotation = config_fields[field].annotation.__name__
                else:
                    # etc.
                    annotation = str(config_fields[field].annotation)

                if not config_fields[field].is_required() and 'Optional' not in str(annotation):  # noqa: E501
                    def_ += f'\n\t{field}: Optional["{annotation}"]' + " = None,"
                elif not config_fields[field].is_required():
                    def_ += f'\n\t{field}: "{annotation}"' + " = None,"
                else:
                    def_ += f'\n\t{field}: "{annotation}"' + ","

        for rkey, rtype in reserved_config_fields:
            def_ += f'\n\t{rkey}: Optional["{rtype}"]' + " = None,"

        if config_model:
            def_ += f'\n\tconfig: Optional["{config_model}"] = None, '
        else:
            def_ += '\n\tconfig: Optional[dict] = None, '
        def_ += '\n\t**extra_config_fields: dict[str, str], '
        def_ = def_[:-2] + ") -> malevich.annotations.OpResult:"
        def_ += f'\n\t"""{self.docstrings}"""\n'
        return def_.replace('\t', ' ' * 4)


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
                    input_file_type='jsonschema'
                )
                out_script =  open(out.name).read().replace(
                    'from __future__ import annotations',
                    '\n'
                )
                return (
                    re.search(
                        r'class (?P<ClassName>\w+)',
                        out_script
                    ).group('ClassName'),   # 1
                    out_script              # 2
                )


    def __init__(
        self,
        path: str,
        malevich_app_name: str | None = None,
        dependency: Dependency | None = None
    ) -> None:
        with chdir(path):
            assert Path('index.yaml').exists(
            ), "No index.yaml found in the package"
            self.index = pydml.parse_yaml_file_as(StubIndex, 'index.yaml')
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

        processors = app_info.processors
        processors = {str(key): value for key, value in processors.items()}

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
            for name, processor in processors.items()
        }

        with chdir(path):
            with open('scheme.py', 'w+') as f_scheme:
                i = 0
                for (name, (class_name, stub,)), proc in zip(
                    config_stubs.items(), processors.values()
                ):
                    if class_name is None or stub is None:
                        continue

                    j = f_scheme.write(stub + '\n')
                    index.schemes.append(
                        StubSchema(
                            name=name,
                            scheme=json.dumps(proc.contextClass),
                            class_name=class_name,
                        )
                    )
                    index.schemes_index[name] = (i, i + j)
                    i += j

                for name, schema in app_info.schemes.items():
                    stub_ = Stub.Utils.generate_context_schema(schema)
                    j = f_scheme.write(stub_[1] + '\n')
                    index.schemes.append(
                        StubSchema(
                            name=name,
                            scheme=schema,
                            class_name=stub_[0]
                        )
                    )
                    index.schemes_index[name] = (i, i + j)

        importlib.import_module(f'malevich.{package_name}.scheme')
        config_models = {
            name: eval(f'malevich.{package_name}.scheme.{config_stubs[name][0]}')
            if config_stubs[name][0] else None
            for name in processors
        }

        functions: dict[str, StubFunction] = {}
        schemes = {*app_info.schemes.values()}

        for name, processor in processors.items():
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
            )
            functions[name].definition = functions[name].generate_definition(
                config_model=config_stubs[name][0]
            )

        with chdir(path):
            with open('F.py', 'w+') as f_F:  # noqa: N806
                i = f_F.write(Templates.imports)

                for name, function in functions.items():
                    j = f_F.write(Templates.registry.format(
                        operation_id=operation_ids[name],
                        registry_record=registry_records[name]
                    ))

                    j += f_F.write(Templates.processor.format(
                        name=name,
                        package_id=package_name,
                        operation_id=operation_ids[name],
                        use_sinktrace=bool(function.sink),
                        definition=function.definition,
                        config_model=config_stubs[name][0],
                    ))
                    index.functions.append(function)
                    index.functions_index[name] = (i, i + j)
                    i += j

            # yaml.dump(index.model_dump(), open('index.yaml', 'w+'))
            pydml.to_yaml_file('index.yaml', index)
            with open('__init__.py', 'w+') as init:
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
