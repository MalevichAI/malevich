from malevich_space.schema import LoadedComponentSchema

from malevich._utility import Stub
from malevich.core_api import AppFunctionsInfo
from malevich.manifest import manf
from malevich.models.dependency import Dependency
from malevich.path import Paths

from .installer import Installer
from .space import SpaceDependency, SpaceDependencyOptions


class IsolatedSpaceInstaller(Installer):
    def install(
        self,
        package_id: str,
        component: LoadedComponentSchema,
        app_info: AppFunctionsInfo
    ) -> None:

        if component.app:
            iauth, itoken = manf.put_secrets(
                image_auth_user=component.app.container_user,
                image_auth_password=component.app.container_token,
            )
        else:
            iauth, itoken = None, None

        dependency = SpaceDependency(
            package_id=package_id,
            options=SpaceDependencyOptions(
                reverse_id=component.reverse_id,
                image_ref=component.app.container_ref,
                image_auth_user=iauth,
                image_auth_pass=itoken,
            )
        )

        Stub.from_app_info(
            app_info=app_info,
            path=Paths.module(package_id),
            package_name=package_id,
            dependency=dependency,
            operation_ids={
                op.core_id: op.uid
                for op in component.app.ops
            },
            registry_records={
                str(op.core_id): {
                    "operation_id": op.uid,
                    "reverse_id": component.reverse_id,
                    "branch": component.branch.uid,
                    "version": component.version.uid,
                    "processor_name": op.core_id,
                    "processor_id":  op.core_id,
                    "image_ref": ('dependencies', package_id, 'options', 'image_ref'),
                    "image_auth_user": (
                        'dependencies', package_id, 'options', 'image_auth_user'
                    ),
                    "image_auth_pass": (
                        'dependencies', package_id, 'options', 'image_auth_pass'
                    )
                }
                for op in component.app.ops
            },
            description=component.description
        )

    def restore(self, dependency: Dependency) -> Dependency:
        return None

    def construct_dependency(self, object: dict) -> Dependency:
        return None
