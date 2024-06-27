import os

from jinja2 import Environment, FileSystemLoader

from ..models.dependency import Integration
from ..path import Paths


class FlowStub:
    @staticmethod
    def sync_flows(
        reverse_id: str,
        integrations: list[Integration],
    )-> None:
        install_path = Paths.module('flows', reverse_id + '.py')
        environment = Environment(
            loader=FileSystemLoader(Paths.templates('metascript', 'flow_stub'))
        )
        with open(install_path, 'w+') as f:
            f.write(
                environment.get_template('body.jinja2').render(
                   reverse_id=reverse_id,
                   integrations=integrations,
                )
            )

        init_path = Paths.module('flows', '__init__.py')
        with open(init_path, 'a+') as f:
            f.write(
                f'from .{reverse_id} import {reverse_id}\n'
            )

    @staticmethod
    def remove_stub(reverse_id: str) -> None:
        os.remove(Paths.module('flows', reverse_id + '.py'))
        with open(Paths.module('flows', '__init__.py')) as f:
            lines = f.readlines()

        with open(Paths.module('flows', '__init__.py'), 'w') as f:
            for line in lines:
                if f'from .{reverse_id} import {reverse_id}' in line:
                    continue
                f.write(line)
