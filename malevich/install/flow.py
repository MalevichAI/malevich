from malevich.models.manifest import Dependency

from .._utility.flow_stub import FlowStub
from ..install.installer import Installer
from ..manifest import ManifestManager
from ..models.dependency import Integration

manf = ManifestManager()

class FlowInstaller(Installer):

    def install(self, reverse_id: str, integrations: list[Integration]) -> None:
        FlowStub.sync_flows(
            reverse_id,
            integrations=integrations
        )

    def restore(self, reverse_id: str, integrations: list[Integration]) -> None:
        return self.install(
            reverse_id,
            integrations
        )

    def construct_dependency(self, object: dict) -> None:
        return None
