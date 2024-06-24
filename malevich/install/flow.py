from .._utility.flow_stub import FlowStub
from ..install.installer import Installer
from ..models.dependency import Integration


class FlowInstaller(Installer):

    def install(self, reverse_id: str, integration: Integration)-> None:
        if not FlowStub.check_installed(
            reverse_id,
            integration
        ):
            FlowStub.write_flow(
                reverse_id,
                integration
            )
        else:
            raise Exception(
                f"Integration {reverse_id} with version "
                f"{integration.version} already exists."
            )

    def restore(self, reverse_id: str, integration: Integration) -> None:
        return self.install(
            reverse_id,
            integration
        )

    def remove(self, reverse_id: str, integration: Integration | None = None) -> None:
        if FlowStub.check_installed(
            reverse_id,
            integration
        ):
            FlowStub.remove_flow(
                reverse_id,
                integration
            )
        else:
            raise Exception(
                f"Integration {reverse_id} with version "
                f"{integration.version} was not found."
            )

    def construct_dependency(self) -> None:
        ...
