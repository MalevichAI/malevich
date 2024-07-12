
from malevich._utility import FlowStub
from malevich.install.installer import Installer
from malevich.manifest import ManifestManager
from malevich.models.dependency import Integration

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
