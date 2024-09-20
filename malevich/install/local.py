import hashlib
from pathlib import Path

from malevich_app import LocalRunner, LocalRunStruct
from malevich._utility.stub import Stub
from malevich.models.installers.local import LocalDependency, LocalOptions
from malevich.models.manifest import Dependency
from malevich.path import Paths

from .installer import Installer


class LocalInstaller(Installer):
    """Installs package using local files"""
    name = "local"

    def install(self, package_name: str, app_path: Path) -> LocalDependency:
        if not app_path.exists():
            raise FileNotFoundError(f"The path {app_path} does not exist")
        if not app_path.is_dir():
            raise NotADirectoryError(f"The path {app_path} is not a directory")

        dependencies = []
        if (app_path / "requirements.txt").exists():
            dependencies = open(app_path / "requirements.txt").readlines()

        dependency = LocalDependency(
            package_id=package_name,
            options=LocalOptions(import_path=app_path, dependencies=dependencies),
        )

        runner = LocalRunner(
            LocalRunStruct(import_dirs=[str(app_path.absolute())])
        )

        operation_ids = {
            str(op.id): hashlib.sha256(op.model_dump_json().encode()).hexdigest()
            for op in [*runner.app_info.processors.values(), *runner.app_info.conditions.values()]
        }

        operation_names = {
            str(op.id): op.name
            for op in [*runner.app_info.processors.values(), *runner.app_info.conditions.values()]
        }

        Stub.from_app_info(
            app_info=runner.app_info,
            path=Paths.module(package_name),
            package_name=package_name,
            dependency=dependency,
            operation_ids=operation_ids,
            registry_records={
                processor_id: {
                    "operation_id": operation_id,
                    "processor_id": operation_names[processor_id],
                    "package_path": str(app_path.absolute())
                }
                for processor_id, operation_id in operation_ids.items()
            },
        )

        return LocalDependency(
            package_id=package_name,
            options=LocalOptions(import_path=app_path, dependencies=dependencies),
        )

    def construct_dependency(self, object: dict) -> Dependency:
        return LocalDependency(**object)

    def restore(self, dependency: Dependency) -> Dependency:
        if not isinstance(dependency, LocalDependency):
            raise ValueError(f"Invalid dependency type: {type(dependency)}")

        return self.install(
            package_name=dependency.package_id,
            app_path=Path(dependency.options.import_path),
        )
