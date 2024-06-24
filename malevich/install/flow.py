from .._utility.flow_stub import FlowStub
from ..install.installer import Installer
from ..manifest import ManifestManager
from ..models.dependency import Integration

manf = ManifestManager()

class FlowInstaller(Installer):

    def install(self, reverse_id: str, integration: Integration)-> None:
        manf_flows = manf.query('flows')
        installed = False
        if reverse_id in manf_flows:
            for i in manf_flows[reverse_id]:
                if i['version'] == integration.version:
                    installed = True
                    if FlowStub.check_installed(reverse_id, integration):
                        raise Exception(
                            f"Integration {reverse_id} with version "
                            f"{integration.version} already exists."
                        )

        if not FlowStub.check_installed(reverse_id, integration):
            FlowStub.write_flow(
                reverse_id,
                integration
            )

        if not installed:
            if reverse_id in manf_flows:
                manf_flows[reverse_id].append(integration.model_dump())
            else:
                manf_flows[reverse_id] = [integration.model_dump()]
        manf.put(
            'flows',
            value=manf_flows
        )
        manf.save()

    def restore(self, reverse_id: str, integration: Integration) -> None:
        return self.install(
            reverse_id,
            integration
        )

    def remove(self, reverse_id: str, integration: Integration | None = None) -> None:
        manf_flows = manf.query('flows')
        installed = True
        if reverse_id not in manf_flows:
            installed = False
            if FlowStub.check_installed(reverse_id):
                FlowStub.remove_flow(
                    reverse_id
                )
                return
            else:
                raise Exception(
                    f"Failed to remove [yellow]{reverse_id}[/yellow]: "
                    f"No such flows installed."
                )

        flows: list = manf_flows[reverse_id]
        if len(flows) < 2 or integration is None:
            if FlowStub.check_installed(reverse_id):
                FlowStub.remove_flow(
                    reverse_id
                )
            if installed:
                manf_flows.pop(reverse_id)
        else:
            idx = None
            for i, v in enumerate(flows):
                if v['version'] == integration.version:
                    integration.mapping = v['mapping']
                    FlowStub.remove_flow(reverse_id, integration)
                    idx = i
                    break
            else:
                raise Exception(
                    f"Version [blue]{integration.version}[/blue] was "
                    "not found in manifest."
                )
            if idx is not None:
                flows.pop(idx)
            manf_flows[reverse_id] = flows
        manf.put('flows', value=manf_flows)
        manf.save()

    def construct_dependency(self) -> None:
        ...
