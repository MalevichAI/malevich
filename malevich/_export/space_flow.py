import re
from collections import defaultdict
from typing import overload

from malevich_space.ops.space import SpaceOps
from malevich_space.schema import (
    LoadedFlowSchema,
    LoadedInFlowComponentSchema,
    SpaceSetup,
)

from malevich._autoflow.tree import ExecutionTree
from malevich._utility import resolve_setup
from malevich.manifest import manf


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
        self.reverse_id = reverse_id
        self._alias_factory = defaultdict(lambda: 1)
        self._alias_memory = set()

        if flow is not None:
            self.flow = flow
            return

        if setup is None:
            setup = resolve_setup(manf.query("space", resolve_secrets=True))
        ops = SpaceOps(setup)

        component_ = ops.get_parsed_component_by_reverse_id(reverse_id=reverse_id)

        if component_ is None or component_.flow is None:
            raise ValueError(f"{reverse_id} does not exists or not a flow")

        self.flow = component_.flow

    def _make_alias(self, component: LoadedInFlowComponentSchema):
        x = f'{component.reverse_id}_{self._alias_factory[component.reverse_id]}'
        while x in self._alias_memory:
            self._alias_factory[component.reverse_id] += 1
            x = f'{component.reverse_id}_{self._alias_factory[component.reverse_id]}'
        self._alias_memory.add(x)
        return x

    def _varname(self, component: LoadedInFlowComponentSchema)-> str:
        # TODO: Make with LLM
        return re.sub(
            r'[\W\s]+',
            '_',
            component.alias or component.reverse_id
        ).lower()

    def _decl(self, varname: str, fn: str, *args, **kwargs):
        kwargs_ = [f'{k}={v!r}' for k, v in kwargs.items()]
        args_ = ', '.join([*map(repr, args), *kwargs_])

        decl = f'{varname} = {fn}({args_})'
        return decl

    def get_meta_pipeline(
        self,
        include_def: bool = True,
        include_decorator: bool = True,
        include_return: bool = True,
        reverse_id: str | None = None,
    ):
        assert reverse_id or self.reverse_id, (
            '`reverse_id` is required if not set in the constructor'
        )
        reverse_id = reverse_id or self.reverse_id
        apps = set([x.reverse_id for x in self.flow.components if x.app is not None])

        body = []
        for component in self.flow.components:
            if not component.alias:
                component.alias = self._make_alias(component)
            else:
                self._alias_memory.add(component.alias)

        varnames = {
            x.alias: self._varname(x)
            for x in self.flow.components
        }
        uid2alias = {}
        for component in self.flow.components:
            uid2alias[component.uid] = component.alias

        collections = [
            x
            for x in self.flow.components
            if x.collection is not None
        ]

        body.extend([
            (node.uid, self._decl(
                varnames[node.alias],
                'collection',
                node.reverse_id,
                alias=node.alias,
            ))
            for node in collections
        ])

        operations = [
            (x, *x.prev)
            for x in self.flow.components
            if x.app is not None
        ]

        config_repr = {
            x.uid: x.active_cfg.cfg_json if x.active_cfg is not None else {}
            for x in self.flow.components
        }

        body.extend([
            (
                node.uid,
                self._decl(
                    varnames.get(node.alias),
                    node.app.active_op[0].name,
                    *[varnames.get(uid2alias.get(x.uid)) for x in y],
                    **config_repr.get(node.uid, {}),
                )
            )
            for node, *y in operations
        ])

        instructions = dict(body)
        tree = ExecutionTree()
        for node in self.flow.components:
            for index, prev in enumerate(node.prev):
                tree.put_edge(prev.uid, node.uid, index)

        ordered_instructions = [
            instructions[x]
            for x in tree.topsort()
        ]

        if include_return:
            ordered_instructions.append(
                'return ' + ', '.join([
                    varnames[uid2alias[x]] for x in tree.leaves()
                ])
            )

        imports_ = (
            'from malevich import flow, collection, table\n'
            + '\n'.join(
                f'from malevich.{x} import *'
                for x in apps
            )
        )


        imports_ = (
            'from malevich import flow, collection, table\n'
            + '\n'.join(
                f'from malevich.{x} import *'
                for x in apps
            )
        )

        body_ = '\n\t'.join(ordered_instructions)
        reverse_id_ = re.sub(r"[\W\s]+", "_", reverse_id).lower()
        def_ = f'def {reverse_id_}():'
        if include_decorator:
            def_ = f'@flow\n{def_}'
        if include_def:
            return f'{def_}\n\t{body_}', imports_
        return body_, imports_
