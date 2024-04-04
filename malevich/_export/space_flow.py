import re
from collections import Counter, defaultdict
from typing import overload

from malevich_space.ops.space import SpaceOps
from malevich_space.schema import (
    LoadedFlowSchema,
    LoadedInFlowComponentSchema,
    SpaceSetup,
)

from .._autoflow.tree import ExecutionTree
from .._utility.space.space import resolve_setup
from ..manifest import manf
from ..models.nodes.operation import OperationNode


class SpaceFlowExporter:
    @overload
    def __init__(self, reverse_id: str, setup: SpaceSetup | None) -> None:
        pass

    @overload
    def __init__(self, flow: LoadedFlowSchema) -> None:
        pass

    def __init__(
        self,
        reverse_id: str | None = None,
        setup: SpaceSetup | None = None,
        flow: LoadedFlowSchema | None = None
    ):
        assert reverse_id is not None or flow is not None, (
            'Requires `flow` or `reverse_id` to be not None'
        )
        if flow is not None:
            self.flow = flow
            return

        if setup is None:
            setup = resolve_setup(manf.query("space", resolve_secrets=True))
        ops = SpaceOps(setup)

        component_ = ops.get_parsed_component_by_reverse_id(reverse_id=reverse_id)

        if component_.flow is None:
            raise ValueError(f"{reverse_id} is not a flow")
        self.flow = component_.flow

        self.alias_factory = defaultdict(lambda: 1)

    def _varname(self, component: LoadedInFlowComponentSchema)-> str:
        # TODO: Make with LLM
        return re.sub(
            r'[\W]+',
            '_',
            component.alias or component.reverse_id
        ).lower()

    def _decl(self, varname: str, fn: str, *args, **kwargs):
        kwargs_ = [f'{k}={v!r}' for k, v in kwargs.items()]
        args_ = ', '.join([*args, *kwargs_])

        decl = f'{varname} = {fn}({args_})'
        return decl

    def _alias(self, x: LoadedInFlowComponentSchema):
        if x.alias is not None:
            return x.alias
        else:
            id_ = self.alias_factory[x.reverse_id]
            self.alias_factory[x.reverse_id] += 1
            x.alias = x.reverse_id + '_' + str(id_)
            return x.alias


    def get_meta_pipeline(
        self,
        include_def: bool = True,
        include_decorator: bool = True,
        reverse_id: str | None = None,
    ):
        body = []
        print([x.reverse_id for x in self.flow.components if x.alias is None])
        varnames = {
            x.alias: self._varname(x)
            for x in self.flow.components
        }
        collections = [
            x
            for x in self.flow.components
            if x.collection is not None
        ]

        body.extend([
            self._decl(
                varnames[node.alias],
                'collection',
                alias=self,
            )
            for node in collections
        ])

        # tree = ExecutionTree()
        # for component in self.flow.components:
        #     for index,  prev in enumerate(component.prev):
        #         tree.put_edge((component, prev, index))

        operations = [
            (x, *x.prev)
            for x in self.flow.components
            if x.app is not None
        ]
        body.extend([
            self._decl(
                varnames.get(node.alias),
                node.app.active_op[0].name,
                *[varnames.get(x.alias) for x in y]
            ) for node, *y in operations
        ])

        return body, ''