import re
from typing import Optional

from malevich_space.schema import LoadedComponentSchema

from .._core.scan import scan_core
from .._utility.space.auto_space_ops import get_auto_ops
from .._utility.stub import Stub
from ..constants import DEFAULT_CORE_HOST
from ..manifest import ManifestManager
from ..models.installers.space import SpaceDependency, SpaceDependencyOptions
from ..path import Paths
from .installer import Installer

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
            self.__ops = get_auto_ops()
        except Exception as e:
            from malevich._cli.space.login import login
            if not login():
                raise Exception(
                    "Login failed. Run `malevich space login` to try again"
                ) from e
            SpaceInstaller.__init__(self)


    def install(
        self,
        package_name: str,
        reverse_id: str,
        branch: Optional[str] = None,
        version: Optional[str] = None,
    ) -> SpaceDependency | LoadedComponentSchema:
        package_name = re.sub(r'[\W\s]+', '_', package_name)

        component = self.__ops.get_parsed_component_by_reverse_id(
            reverse_id=reverse_id
        )

        if component is None:
            raise Exception(f"Component {reverse_id} not found")

        if component.flow is not None:
            return component

        if component.app is None:
            raise Exception(f"Component {reverse_id} is not an app")


        m = ManifestManager()

        if component.app:
            iauth, itoken = m.put_secrets(
                image_auth_user=component.app.container_user,
                image_auth_password=component.app.container_token,
            )
        else:
            iauth, itoken = None, None

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

        Stub.from_app_info(
            app_info=scan_core(
                image_ref=component.app.container_ref,
                image_auth=(
                    component.app.container_user, component.app.container_token
                ),
                core_host=DEFAULT_CORE_HOST,
            ),
            path=Paths.module(package_name),
            package_name=package_name,
            dependency=dependency,
            operation_ids={
                op.core_id: op.uid
                for op in component.app.ops
            },
            registry_records={
                str(op.core_id): {
                    "operation_id": op.uid,
                    "reverse_id": reverse_id,
                    "branch": component.branch.uid,
                    "version": component.version.uid,
                    "processor_name": op.core_id,
                    "processor_id":  op.core_id,
                    "image_ref": ('dependencies', package_name, 'options', 'image_ref'),
                    "image_auth_user": (
                        'dependencies', package_name, 'options', 'image_auth_user'
                    ),
                    "image_auth_pass": (
                        'dependencies', package_name, 'options', 'image_auth_pass'
                    )
                }
                for op in component.app.ops
            },
            description=component.description
        )

        return dependency

    def restore(self, dependency: SpaceDependency) -> SpaceDependency | LoadedComponentSchema:
        return self.install(
            package_name=dependency.package_id,
            reverse_id=dependency.options.reverse_id,
            branch=dependency.options.branch,
            version=dependency.options.version
        )

    def construct_dependency(self, dependency: dict) -> SpaceDependency:
        return SpaceDependency(**dependency)
