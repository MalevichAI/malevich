"""
Provides functionality to install packages from plain Docker images
using Malevich Core capabilities
"""

import hashlib
import json
from typing import Optional

import malevich_coretools as core

from .._utility.host import fix_host
from ..constants import DEFAULT_CORE_HOST
from ..install.installer import Installer
from ..manifest import ManifestManager
from ..models.installers.image import ImageDependency, ImageOptions
from .mimic import mimic_package

_pydantic_types = {
    "string": str.__name__,
    "integer": int.__name__,
    "number": float.__name__,
    "boolean": bool.__name__,
}


class Templates:
    """Templates for creating a package from a Docker image"""

    disclaimer = """\"\"\"
THIS FILE IS AUTOGENERATED BY malevich PACKAGE INSTALLER.

THIS FILE CONTAINS VITAL INFORMATION ABOUT THE PACKAGE AND ITS CONTENTS.
DO NOT MODIFY THIS FILE MANUALLY.
\"\"\"
"""

    imports = """
from malevich._autoflow.function import autotrace, sinktrace
from malevich._utility.registry import Registry
from malevich.models.nodes import OperationNode

from pydantic import BaseModel
"""
    registry = """
Registry().register("{operation_id}", {{
    "image_ref": {image_ref},
    "image_auth_user": {image_auth_user},
    "image_auth_pass": {image_auth_pass},
    "processor_id": "{processor_id}",
}})
"""

    schema = """
class {schema_name}(BaseModel):
{schema_def}
"""

    processor = """
@{decor}
def {name}({args}config: dict = {{}}):
    \"\"\"{docs}\"\"\"
    return OperationNode(operation_id="{operation_id}", config=config)
"""
    init = """
# {init_name} = ...

"""

    input = """
# {input_name} = ...

"""

    output = """
# {output_name} = ...

"""


class ImageInstaller(Installer):
    """Installs package using Docker image"""
    name = "image"

    @staticmethod
    def _scan_core(
        core_auth: tuple[str, str],
        core_host: str,
        image_ref: str,
        image_auth: tuple[str, str],
    ) -> core.abstract.AppFunctionsInfo:
        """Scans Core for image info

        Args:
            core_auth: Core credentials
            core_host: Core host
            image_ref: Docker image reference
            image_auth: Docker image credentials
        """
        try:
            info = core.get_image_info(
                image_ref,
                image_auth=image_auth,
                conn_url=fix_host(core_host),
                auth=core_auth,
                parse=True
            )
        except Exception as err:
            raise Exception(
                f"Can't get info for image {image_ref}. Check that "
                "image exists and credentials are correct.\n"
                f"Error: {err}"
            )
        else:
            return info

    @staticmethod
    def create_operations(
        operations: core.abstract.AppFunctionsInfo,
        package_name: str,
    ) -> str:
        """Generates Python code for package creation

        Args:
            operations: Operations info
            package_name: Name of the package to be created
        """
        salt = hashlib.sha256(
            operations.model_dump_json().encode()).hexdigest()
        contents = Templates.disclaimer
        contents += Templates.imports

        for schema_name, schema_fields in operations.schemes.items():
            fields = json.loads(schema_fields)["properties"]
            field_types = {
                k: _pydantic_types.get(v["type"], "str")
                for k, v in fields.items()
                if "type" in v.keys()
            }

            schema_definition = "\n".join(
                [f"\t{k}: {v} = None" for k, v in field_types.items()
                 if k and v]
            )
            if not schema_definition:
                schema_definition = "\tpass"

            contents += Templates.schema.format(
                schema_name=schema_name,
                schema_def=schema_definition,
            )

        for id_, processor in operations.processors.items():
            args_ = []
            args_str_ = ""

            for arg_ in processor.arguments:
                if "return" in arg_[0] or (arg_[1] and "Context" in arg_[1]) or (arg_[1] and "Sink" in arg_[1]):
                    continue
                schema = None
                for name in operations.schemes.keys():
                    if arg_[1] and name in arg_[1]:
                        schema = name
                        break
                args_.append(f"{arg_[0]}{': ' + schema if schema else ''}")

                args_str_ = ", ".join(args_)
                args_str_ += ", " if args_str_ else ""

            if sink := any([arg_[1] and 'Sink' in arg_[1] for arg_ in processor.arguments]):
                args_str_ += "*args, "

            checksum = hashlib.sha256(
                processor.model_dump_json().encode()
            ).hexdigest()

            # indexed_operations[checksum] = id_

            contents += Templates.registry.format(
                operation_id=checksum,
                image_ref=("dependencies", package_name,
                           "options", "image_ref"),
                image_auth_user=(
                    "dependencies",
                    package_name,
                    "options",
                    "image_auth_user",
                ),
                image_auth_pass=(
                    "dependencies",
                    package_name,
                    "options",
                    "image_auth_pass",
                ),
                processor_id=id_,
            )
            contents += Templates.processor.format(
                name=id_,
                args=args_str_,
                operation_id=checksum,
                docs=processor.doc,
                decor="sinktrace" if sink else "autotrace"
            )

        for id_, input_ in operations.inputs.items():
            contents += Templates.input.format(input_name=id_)

        for id_, init_ in operations.inits.items():
            contents += Templates.init.format(init_name=id_)

        for id_, output_ in operations.outputs.items():
            contents += Templates.output.format(output_name=id_)

        return contents  # , indexed_operations

    def install(
        self,
        package_name: str,
        image_ref: str,
        image_auth: tuple[str, str],
        core_host: str = DEFAULT_CORE_HOST,
        core_auth: Optional[tuple[str, str]] = None,
    ) -> ImageDependency:
        app_info = ImageInstaller._scan_core(
            core_auth=core_auth,
            core_host=core_host,
            image_ref=image_ref,
            image_auth=image_auth,
        )
        checksum = hashlib.sha256(
            app_info.model_dump_json().encode()).hexdigest()
        # metascript, operations = ImageInstaller.create_operations(
        metascript = ImageInstaller.create_operations(
            app_info, package_name
        )

        mimic_package(
            package_name,
            metascript,
        )

        m = ManifestManager()
        iauth_user, iauth_pass, cauth_user, cauth_token = m.put_secrets(
            image_auth_user=image_auth[0],
            image_auth_password=image_auth[1],
            core_auth_user=core_auth[0] if core_auth else None,
            core_auth_token=core_auth[1] if core_auth else None,
            salt=checksum,
        )

        return ImageDependency(
            package_id=package_name,
            version="",
            installer="image",
            options=ImageOptions(
                checksum=checksum,
                core_host=core_host,
                core_auth_token=cauth_token,
                core_auth_user=cauth_user,
                image_auth_user=iauth_user,
                image_auth_pass=iauth_pass,
                image_ref=image_ref,
                # operations=operations,
            ),
        )

    def restore(self, dependency: ImageDependency) -> None:
        core_user = dependency.options.core_auth_user
        core_token = dependency.options.core_auth_token

        return self.install(
            package_name=dependency.package_id,
            image_ref=dependency.options.image_ref,
            image_auth=(
                dependency.options.image_auth_user or "",
                dependency.options.image_auth_pass or ""
            ),
            core_host=dependency.options.core_host,
            core_auth=(
                core_user,
                core_token,
            ) if core_user and core_token else None,
        )

    def construct_dependency(self, object: dict) -> ImageDependency:
        manf = ManifestManager()
        parsed = ImageDependency(**object)
        parsed.options.image_auth_pass = manf.query_secret(
            parsed.options.image_auth_pass,
            only_value=True,
        )
        parsed.options.image_auth_user = manf.query_secret(
            parsed.options.image_auth_user,
            only_value=True,
        )
        parsed.options.core_auth_user = manf.query_secret(
            parsed.options.core_auth_user,
            only_value=True,
        )
        parsed.options.core_auth_token = manf.query_secret(
            parsed.options.core_auth_token,
            only_value=True,
        )
        return parsed
