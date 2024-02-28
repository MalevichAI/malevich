import re
from typing import Optional

from malevich_space.ops import SpaceOps

from .._utility.space.space import resolve_setup
from ..manifest import ManifestManager
from ..models.installers.space import SpaceDependency, SpaceDependencyOptions
from .installer import Installer
from .stub import create_package_stub

manf = ManifestManager()


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
    "reverse_id": "{reverse_id}",
    "branch": {branch},
    "version": {version},
    "processor_name": "{name}",
    "processor_id": "{name}",
    "image_ref": {image_ref},
    "image_auth_user": {image_auth_user},
    "image_auth_pass": {image_auth_pass},
}})
"""

    processor = """
@{decor}
def {name}({args}config: dict = {{}}):
    \"\"\"{docs}\"\"\"
    return OperationNode(
        operation_id="{operation_id}",
        config=config,
        processor_id="{name}",
        package_id="{package_id}",
    )
"""


class SpaceInstaller(Installer):
    name = 'space'

    def __init__(self) -> None:
        super().__init__()
        try:
            self.__ops = SpaceOps(resolve_setup(
                manf.query('space', resolve_secrets=True)
            ))
        except Exception as e:
            raise Exception(
                "Setup is invalid. Run `malevich space login`") from e

    def install(
        self,
        package_name: str,
        reverse_id: str,
        branch: Optional[str] = None,
        version: Optional[str] = None,
    ) -> SpaceDependency:

        component = self.__ops.get_parsed_component_by_reverse_id(
            reverse_id=reverse_id
        )

        if component is None:
            raise Exception(f"Component {reverse_id} not found")

        if component.app is None:
            raise Exception(f"Component {reverse_id} is not an app")

        metascript = Templates.disclaimer
        metascript += Templates.imports

        m = ManifestManager()

        if component.app:
            iauth, itoken = m.put_secrets(
                image_auth_user=component.app.container_user,
                image_auth_password=component.app.container_token,
            )
        else:
            iauth, itoken = None, None

        for op in component.app.ops:
            if op.type != "processor":
                continue
            metascript += Templates.registry.format(
                operation_id=op.uid,
                reverse_id=reverse_id,
                branch=str(component.branch.model_dump()),
                version=str(component.version.model_dump()),
                name=op.core_id,
                image_ref=(
                    "dependencies",
                    package_name,
                    "options",
                    "image_ref"
                ),
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
            )
            is_sink = any(
                ['Sink' in arg_.arg_type
                 for arg_ in op.args if arg_.arg_type
            ])
            args_ = []

            for arg_ in op.args:
                if "return" in arg_.arg_name \
                        or (arg_.arg_type and "Context" in arg_.arg_type):
                    continue
                args_.append(arg_.arg_name)

            metascript += Templates.processor.format(
                name=op.core_id,
                args=(", ".join(args_) + ", " if args_ else "") if not is_sink else '*args, ',  # noqa: E501
                docs=op.doc,
                operation_id=op.uid,
                decor='autotrace' if not is_sink else 'sinktrace',
                package_id=package_name
            )

        dependency = SpaceDependency(
            package_id=package_name,
            version=component.version.readable_name,
            installer="space",
            options=SpaceDependencyOptions(
                reverse_id=reverse_id,
                branch=component.branch.uid,
                version=component.version.uid,
                image_ref=component.app.container_ref,
                image_auth_user=iauth,
                image_auth_pass=itoken
            )
        )

        create_package_stub(
            package_name=re.sub(r"[^a-zA-Z0-9_]", "_", package_name),
            metascript=metascript,
            dependency=dependency
        )

        return dependency

    def restore(self, dependency: SpaceDependency) -> SpaceDependency:
        return self.install(
            package_name=dependency.package_id,
            reverse_id=dependency.options.reverse_id,
            branch=dependency.options.branch,
            version=dependency.options.version
        )

    def construct_dependency(self, dependency: dict) -> SpaceDependency:
        return SpaceDependency(**dependency)
