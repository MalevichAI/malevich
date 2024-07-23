
from malevich._dev.singleton import SingletonMeta
from malevich._utility.package import package_manager as pm
from malevich.constants import TEST_DIR
from malevich.install.image import ImageInstaller
from malevich.install.installer import Installer
from malevich.install.space import SpaceInstaller
from malevich.manifest import OverrideManifest, manf
from malevich.models.dependency import Dependency
from malevich.models.installers.compat import CompatabilityStrategy
from malevich.models.preferences import (
    Action,
    LogFormat,
    UserPreferences,
    VerbosityLevel,
)

test_manifest = OverrideManifest(TEST_DIR)

class EnvManager(metaclass=SingletonMeta):
    """Efficient environment manager

    Provides optimized and flexible way for installing,
    tracking and removing environments for testing.
    """
    installers: dict[str, Installer] = {
        'image': ImageInstaller(),
        'space': SpaceInstaller()
    }
    """A set of know installers"""

    def __init__(
        self,
        compatability_strategy: CompatabilityStrategy = None,
    ) -> None:
        """Initialize new manager

        Args:
            compatability_strategy (CompatabilityStrategy):
                A stategy used to compare dependencies
        """
        if not compatability_strategy:
            compatability_strategy = CompatabilityStrategy(
                none_is_always_compatible=True
            )

        self._strategy = compatability_strategy
        self.clean_env()

    def clean_env(self) -> None:
        """Remove all installed stubs and dependencies"""
        with test_manifest:
            stubs, _ = self.get_current_env()
            manf.remove('dependencies')
            manf.put('dependencies', value=[])
            manf.put('preferences', value=UserPreferences(
                verbosity={
                    Action.Interpretation.value: VerbosityLevel.AllSteps.value,
                    Action.Preparation.value: VerbosityLevel.AllSteps.value,
                    Action.Run.value: VerbosityLevel.AllSteps.value,
                    Action.Stop.value: VerbosityLevel.AllSteps.value,
                    Action.Installation.value: VerbosityLevel.AllSteps.value,
                    Action.Removal.value: VerbosityLevel.AllSteps.value,
                    Action.Results.value: VerbosityLevel.AllSteps.value,
                },
                log_format = LogFormat.Plain,
                log_level="DEBUG"
            ))
            for stub in stubs:
                pm.remove_stub(stub[0])

    def get_stubs(self) -> list[tuple[str, str]]:
        """Retrieves a list of stubs

        Returns:
            tuple[str, str]: pairs (stub_name, stub_path)
        """
        all_ = pm.get_all_packages()
        return [(x, pm.get_package_path(x),) for x in all_]

    def get_manifested(self) -> list[Dependency]:
        """Retrieves a list of manifested dependencies

        Returns:
            list[Dependency]: a list of dependencies found in manifest
        """
        deps = []
        with test_manifest:
            for x in manf.query('dependencies'):
                obj = x[next(iter(x.keys()))]
                deps.append(self.installers[obj['installer']].construct_dependency(obj))
        return deps

    def get_current_env(self) -> tuple[list[str], list[Dependency]]:
        """Retrieves stubs and manifested dependencies"""
        stubs = self.get_stubs()
        manifested = self.get_manifested()
        return stubs, manifested

    def request_env(self, dependencies: list[Dependency]) -> None:
        """Provides a contract for a new environment"""
        _, manifested = self.get_current_env()
        for dependency in dependencies:
            # should_offload = False
            for m in manifested:
                if (
                    dependency.package_id == m.package_id
                    and dependency.compatible_with(m, self._strategy)
                ):
                    break
            else:
                with test_manifest:
                    try:
                        pm.remove_stub(dependency.package_id)
                    except Exception:
                        pass
                    manf.remove('dependencies', dependency.package_id)
                    restored = (
                        self.installers[dependency.installer].restore(dependency).model_dump()
                    )

                    manf.put(
                        'dependencies',
                        dependency.package_id,
                        value=restored,
                    )

        return self.get_current_env()
