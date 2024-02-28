
from malevich._utility.package import package_manager as pm
from malevich.install.image import ImageInstaller
from malevich.install.installer import Installer
from malevich.install.space import SpaceInstaller
from malevich.manifest import OverrideManifest, manf
from malevich.models.dependency import Dependency
from ..models.preferences import UserPreferences, VerbosityLevel, Action, LogFormat
from malevich.path import Paths

from ..models.installers.compat import CompatabilityStrategy

test_manifest = OverrideManifest(Paths.home("testing", create=True))

class EnvManager:
    installers: dict[str, Installer] = {
        'image': ImageInstaller(),
        'space': SpaceInstaller()
    }
    def __init__(
        self,
        compatability_strategy: CompatabilityStrategy = None,
    ) -> None:
        if not compatability_strategy:
            compatability_strategy = CompatabilityStrategy(
                none_is_always_compatible=True
            )

        self._strategy = compatability_strategy


    def clean_env(self) -> None:
        with test_manifest:
            stubs, _ = self.get_current_env()
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
        all_ = pm.get_all_packages()
        return [(x, pm.get_package_path(x),) for x in all_]

    def get_manifested(self) -> list[Dependency]:
        deps = []
        with test_manifest:
            for x in manf.query('dependencies'):
                obj = x[next(iter(x.keys()))]
                deps.append(self.installers[obj['installer']].construct_dependency(obj))
        return deps

    def get_current_env(self) -> tuple[list[str], list[Dependency]]:
        stubs = self.get_stubs()
        manifested = self.get_manifested()
        return stubs, manifested

    def request_env(self, dependencies: list[Dependency]) -> None:
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
                    manf.put(
                        'dependencies',
                        value={
                            dependency.package_id:
                            self.installers
                                [dependency.installer]
                                .restore(dependency)
                                .model_dump()
                        },
                        append=True
                    )
        return self.get_current_env()
