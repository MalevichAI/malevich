import json
import os
import logging
from datetime import datetime
from pydantic import BaseModel
from malevich._dev.singleton import SingletonMeta
from malevich.path import Paths
from malevich.models.installers.compat import CompatabilityStrategy
from malevich.install.installer import Installer
from malevich.manifest import ManifestManager
from malevich.models.dependency import Dependency
from malevich.constants import TEST_DIR
from malevich.install.image import ImageInstaller
from malevich.install.space import SpaceInstaller
from malevich._utility import CacheManager, package_manager

env_manf = ManifestManager(TEST_DIR)
cache = CacheManager()

class _OffloadedDependency(BaseModel):
    """When dependency is offloaded, it is saved to this model"""
    orig_stub_path: str
    offloaded_stub_path: str
    dependency: Dependency | None = None

class EnvManager(metaclass=SingletonMeta):
    session = datetime.now().strftime('%d.%m.%y__%H:%M:%S')
    installers: dict[str, Installer] = {
        'image': ImageInstaller(),
        'space': SpaceInstaller()
    }
    def _setup_logger(self) -> None:
        self._logger = logging.getLogger('malevich.testing')
        self._logger.handlers.clear()
        formatter = logging.Formatter(
            "%(asctime)s -- %(funcName)s:%(lineno)d -- %(message)s"
        )
        handler = logging.FileHandler(
            cache.testing.probe_new_entry('protocol.log', entry_group=self.session)[-1],
            mode='a+'
        )
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)

    def __init__(self) -> None:
        self._offloaded: list[_OffloadedDependency] = []
        self._logger = None
        self._setup_logger()


    def offload_stub(  # noqa: ANN201
        self,
        stub_id: str,
        dont_fail: bool = True
    ):
        stub_path = package_manager.get_package_path(stub_id)
        if os.path.exists(stub_path):
            off_path = cache.testing.cache_dir(
                stub_path, entry_group='offload'
            )
            return _OffloadedDependency(
                orig_stub_path=stub_path,
                offloaded_stub_path=off_path,
            )
        elif not dont_fail:
            raise Exception(f"No stub found for {stub_id}")

    def offload_manf(
        self,
        package_name: str,
        dont_fail: bool = False
    ) -> None:
        try:
            env_manf.remove('dependencies', package_name)
        except Exception:
            if not dont_fail:
                raise



    def current_env(self) -> tuple[dict[str, Dependency], list[str]]:
        manifest_keys = [
            next(iter(key.keys()))
            for key in env_manf.query('dependencies')
        ]
        manifested_raw = [
            env_manf.query('dependencies', key)
            for key in manifest_keys
        ]
        manifested: dict[str, Dependency] = {
            key: (
                self.installers[x['installer']]
                .construct_dependency(x)
            )
            for key, x in zip(
                manifest_keys,
                manifested_raw
            )
        }
        stubs = package_manager.get_all_packages()
        return manifested, stubs

    def install(self, dependency: Dependency) -> None:
        installer = dependency.installer
        dependency = self.installers[installer].restore(dependency)
        env_manf.put(
            'dependencies',
            dependency.package_id,
            value={dependency.package_id: dependency.model_dump()},
            append=True
        )


    def request_env(
        self,
        dependencies: list[Dependency | tuple[str, Dependency]]
    ) -> None:
        manifested, _ = self.current_env()
        for record in dependencies:
            if isinstance(record, tuple):
                package_name, dependency = record
            else:
                package_name = record.package_id
                dependency = record

            package_id = dependency.package_id
            to_offload_ = None
            for manifest_name, manifest_dependency in manifested.items():
                if dependency.compatible_with(
                    manifest_dependency,
                    compatability_strategy=CompatabilityStrategy()
                ):
                    break
                if manifest_name == package_name:
                    # Found collision in manifest: should be offloaded
                    to_offload_ = manifest_name, manifest_dependency
            else:
                # If no compatible dependency is found, offload the stub
                off_ = self.offload_stub(package_id)
                if to_offload_:
                    off_.dependency = to_offload_[-1]
                    self.offload_manf(to_offload_[0])
                self._offloaded.append(off_)

                # And install our dependency
                self.install(dependency)
                self._logger.info(
                    f"install {package_id}, dependency= "
                    + json.dumps(dependency.model_dump(), separators=(',', ':'))
                )
