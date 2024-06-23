import os

from .. import flows
from ..models.dependency import Integration


class FlowTemplates:
    imports = """
from typing import Literal, overload

from ..models.results import SpaceCollectionResult
from .stub import integration
"""

    integration = """

@integration(
    mapping={mapping},
    version="{version}",
    reverse_id="{reverse_id}"
)
@overload
def {reverse_id}(
    version: Literal['{version}'],
    {args},
    get_task=False
) -> list[SpaceCollectionResult]:
    ...

"""

    init_import = """

from .{reverse_id} import {reverse_id}

"""

class FlowStub:
    @staticmethod
    def write_flow(
        reverse_id: str,
        integration: Integration
    )-> None:
        from .. import flows
        flows_path_ = os.path.dirname(flows.__file__)
        install_path_ = os.path.join(flows_path_, reverse_id + '.py')
        if not os.path.exists(install_path_):
            with open(install_path_, 'w+') as f:
                f.write(
                    FlowTemplates.imports
                )
        args = [key + "=None" for key in integration.mapping.keys()]
        with open(install_path_, 'a+') as f:
            f.write(
                FlowTemplates.integration.format(
                    mapping=str(integration.mapping),
                    version=integration.version,
                    reverse_id=reverse_id,
                    args='\n\t'.join(args)
                )
            )
        with open(flows.__file__, 'a+') as f:
            f.write(FlowTemplates.init_import.format(reverse_id=reverse_id))

    @staticmethod
    def remove_flow(
        reverse_id: str,
        integration: Integration=None,
    )-> None:
        flows_path_ = os.path.dirname(flows.__file__)
        install_path_ = os.path.join(flows_path_, reverse_id + '.py')
        if integration is None:
            os.remove(install_path_)
            data = open(flows.__file__).read()
            data = data.replace(
                FlowTemplates.init_import.format(reverse_id=reverse_id),
                "",
                1
            )
            with open(flows.__file__, 'w') as f:
                f.write(data)
        else:
            args = [key + "=None" for key in integration.mapping.keys()]
            data = open(install_path_).read()
            data = data.replace(
                FlowTemplates.integration.format(
                    mapping=str(integration.mapping),
                        version=integration.version,
                        reverse_id=reverse_id,
                        args='\n\t'.join(args)
                ),
                "",
                1
            )
            with open(install_path_, 'w') as f:
                f.write(data)

    @staticmethod
    def check_installed(
        reverse_id: str,
        integration: Integration | None
    ) -> bool:
        flows_path_ = os.path.dirname(flows.__file__)
        install_path_ = os.path.join(flows_path_, reverse_id + '.py')
        if not os.path.exists(install_path_):
            return False
        if integration is None:
            return True
        args = [key + "=None" for key in integration.mapping.keys()]
        integration_ = FlowTemplates.integration.format(
            mapping=str(integration.mapping),
            version=integration.version,
            reverse_id=reverse_id,
            args='\n\t'.join(args)
        )
        return integration_ in open(install_path_).read()
