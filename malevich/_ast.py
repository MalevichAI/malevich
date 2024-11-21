import ast
from collections.abc import Iterator
import enum
import inspect
import re
import sys
import traceback
from unittest.mock import call
import uuid
import warnings
import astor
from copy import deepcopy
from typing import Callable, NoReturn

from malevich._autoflow.flow import Flow
from malevich._autoflow.tracer import autoflow, traced
from malevich._autoflow.tree import ExecutionTree
from malevich.models.exceptions import NewVariableInBranchError
from .models.mappers.conditional import AddNegativeCondition, AddPositiveCondition
from malevich.models.nodes.operation import OperationNode

from .models.nodes.base import BaseNode
from .models.nodes.morph import MorphNode

State = tuple[dict, dict] # globals, locals

def copy_state(state: State):
    return state[0], deepcopy(state[1])


def extract_conditioned_nodes(old_tree: ExecutionTree, new_tree: ExecutionTree):
    old_tree_nodes = set(old_tree.nodes())
    new_tree_nodes = set(new_tree.nodes())
    new_nodes = new_tree_nodes.difference(old_tree_nodes)

    for new_node in new_nodes:
        for from_node, _, _ in new_tree.edges_to(new_node):
            if from_node not in old_tree_nodes:
                break
        else:
            yield new_node


def prep_synerr_assign(name: str, stmts: list[ast.stmt]) -> NewVariableInBranchError | None:
    for stmt in stmts:
        for node in ast.walk(stmt):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    for inode in ast.walk(target):
                        if isinstance(inode, ast.Name) and inode.id == name:
                            return NewVariableInBranchError(
                                var_name=name,
                                function_name=None,
                                # FIXME: wrong lineno
                                details=('<string>', stmt.lineno,
                                         target.col_offset, ast.unparse(node))
                            )
    return None


def retrace(tree: ExecutionTree, state):
    for value in state[1].values():
        if isinstance(value, traced):
            value._autoflow._tree_ref = tree

    return state


def merge_detached_tree_inplace(tree: ExecutionTree, detached_tree: ExecutionTree) -> None:
    edges = [*tree.tree, *detached_tree.tree]
    cedges = dict()
    for f, t, l in edges:
        cedges[(f.owner.uuid, t.owner.uuid)] = (f, t, l)

    tree.tree = list(cedges.values())


def combine_branch_states_inplace(state, if_state, else_state, cond_node):
    if else_state is None:
        else_state = state

    initial_state, if_lstate, else_lstate = (
        state[1], if_state[1], else_state[1],
    )

    for key in initial_state:
        if key in if_lstate and key in else_lstate and (
            initial_state[key] is not if_lstate[key]        # key changed in at least
            or initial_state[key] is not else_lstate[key]   # one of the branches
        ):
            v, ifv, elsev = initial_state[key], if_lstate[key], else_lstate[key]
            if (
                all(map(lambda x: isinstance(x, traced) or x is None, (v, ifv, elsev)))
                and all(map(lambda x: isinstance(x.owner, BaseNode) or x is None, (v, ifv, elsev)))
            ):
                ifv_morph = MorphNode.transfigure(ifv.owner, cond_node, True)
                elsev_morph = MorphNode.transfigure(elsev.owner, cond_node, False)
                initial_state[key] = traced(MorphNode.combined(ifv_morph, elsev_morph))
            else:
                raise NotImplementedError(
                    "Malevich does not yet support merging of the states "
                    "of the variables that are not traced."
                )


def exec_flow(
    stmts: list[ast.stmt],
    state,
    filename: str | None = None,
    function_name: str | None = None,
    should_be_true: list[str] | None = None,
    should_be_false: list[str] | None = None,
):
    if should_be_true is None:
        should_be_true = []
    if should_be_false is None:
        should_be_false = []

    return_map = list()

    for stmt in stmts:
        if isinstance(stmt, ast.Return):
            value = ast.Expression(body=stmt.value)
            return_value = eval(
                compile(value, '<string>', 'eval'), state[0], state[1]
            )

            return_map.append(({
                **{s: True for s in should_be_true},
                **{s: False for s in should_be_false}
            } or None, return_value))

            return return_value, state, return_map

        elif isinstance(stmt, ast.If):
            if_expr = ast.Expression(body=stmt.test)

            if_expr_value = eval(
                compile(if_expr, '<string>', 'eval'),
                state[0],
                state[1]
            )

            if not isinstance(if_expr_value, traced) or not isinstance(
                if_expr_value.owner, OperationNode
            ):
                raise SyntaxError(
                    "Condition expression within the flow can only "
                    "be a @condition functions and their boolean combinations. "
                    f"The expression evaluated to `{type(if_expr_value).__name__}`.",
                    (filename, 0, stmt.col_offset, 'if ' + ast.unparse(stmt.test))
                )

            tracers = {*autoflow.get_trace_refs(Flow.flow_ref())}

            b_tree = Flow.flow_ref()
            with Flow() as if_tree:
                # Changing all the traced objects in state before
                # the branch to point to the new tree
                autoflow.retrace(b_tree, if_tree)

                # Mark all future traced objects to be conditioned
                # with node mapper. Also, clone all previous node mappers
                # (for nested conditions)
                mapper_id = 'if' + if_expr_value.owner.uuid

                if_tree.clone_node_mappers(b_tree)
                if_tree.register_node_mapper(
                    AddPositiveCondition(if_expr_value.owner.uuid),
                    key=mapper_id
                )
                # -------------------------------------------------------

                cstate = copy_state(state)

                # Recursively execute the branch
                if_return, if_state, if_map = exec_flow(
                    stmt.body,
                    cstate,
                    filename=filename,
                    should_be_true=[*should_be_true, if_expr_value.owner],
                    should_be_false=should_be_false
                )

                if_tree = Flow.flow_ref()


            if stmt.orelse:
                with Flow() as else_tree:
                    for tracer in tracers:
                        tracer.retrace_self(else_tree)

                    mapper_id = 'else' + if_expr_value.owner.uuid
                    else_tree.clone_node_mappers(b_tree)
                    else_tree.register_node_mapper(
                        AddNegativeCondition(if_expr_value.owner.uuid),
                        key=mapper_id
                    )

                    cstate = copy_state(state)

                    else_return, else_state, else_map = exec_flow(
                        stmt.orelse,
                        cstate,
                        filename=filename,
                        should_be_true=should_be_true,
                        should_be_false=[*should_be_false, if_expr_value.owner]
                    )

                    else_tree = Flow.flow_ref()
            else:
                else_return, else_state, else_tree = None, None, None

            full_tree = ExecutionTree() # Merge all the trees

            for edge in [
                *if_tree.tree,
                *(else_tree.tree if else_tree else []),
                *Flow.flow_ref().tree
            ]:
                # NOTE: List-merge is safe here, but in future
                # it should be encapsulated in special method
                full_tree.put_edge(*edge)

            # Safety first: retrace all the tracers to the new tree
            # and hardset the tree in the flow
            autoflow.retrace(Flow.flow_ref(), full_tree)
            autoflow.retrace(if_tree, full_tree)
            if else_tree:
                autoflow.retrace(else_tree, full_tree)

            # TODO: Hardset is error-prone, need to find a better way
            # with weak refs probably
            Flow._hardset_tree(full_tree)

            if if_return is not None:
                Flow.flow_ref().register_node_mapper(
                    AddNegativeCondition(if_expr_value.owner.uuid)
                )
                return_map.extend(if_map)

                if else_return is None:
                    should_be_false.append(if_expr_value.owner)

            if else_return is not None:
                Flow.flow_ref().register_node_mapper(
                    AddPositiveCondition(if_expr_value.owner.uuid)
                )
                return_map.extend(else_map)

                if if_return:
                    should_be_true.append(if_expr_value.owner)


            # merge_detached_tree_inplace(Flow.flow_ref(), if_tree)
            # if else_tree:
            #     merge_detached_tree_inplace(Flow.flow_ref(), else_tree)

            # state = retrace(Flow.flow_ref(), state)
            combine_branch_states_inplace(state, if_state, else_state, if_expr_value)
        else:
            exec(compile(ast.Module(body=[stmt], type_ignores=[
        ]), '<string>', 'exec'), state[0], state[1])

    return None, state, return_map

class synthetic_tuple(tuple):  # noqa: N801
    def __getattr__(self, item) -> NoReturn:
        if item == '__name__' or item == '__make_tuple':
            return super().__getattribute__(item)
        raise AttributeError(
            "You cannot get an attribute of a result. "
            "Results of function calls are only supposed to be passed as "
            "positional arguments to other functions."
        )

    def __call__(self, *args, **kwargs) -> NoReturn:
        raise TypeError(
            f"You cannot call a result {self.__name__}. "
            "Results of function calls are only supposed to be passed as "
            "positional arguments to other functions."
        )

    def __getitem__(self, item) -> NoReturn:
        raise TypeError(
            "You cannot get an item from a result. "
            "Results of function calls are only supposed to be passed as "
            "positional arguments to other functions. Do not use `result[item]`"
        )

    def __iter__(self) -> Iterator:
        if not self.__make_tuple:
            raise TypeError(
                "You cannot iterate over or unpack a result. "
                "Results of function calls are only supposed to be passed as "
                "positional arguments to other functions."
            )
        return super().__iter__()

    def tuple(self) -> tuple:
        self.__make_tuple = True
        _t = tuple(self)
        self.__make_tuple = False
        return _t

def exec_synthetic_flow(
    stmts: list[ast.stmt],
    state,
    filename: str | None = None,
    function_name: str | None = None,
    should_be_true: list[str] | None = None,
    should_be_false: list[str] | None = None,
):
    collection_args = {}
    document_nodes = {}
    targets = {}

    def synthetic_table(*args, **kwargs):
        return ('__table__', args, kwargs)

    def synthetic_collection(*args, **kwargs):
        uid = uuid.uuid4().hex
        collection_args[uid] = (args, kwargs)
        return synthetic_tuple(('__collection__', uid,))

    def synthetic_document(*args, **kwargs):
        uid = uuid.uuid4().hex
        document_nodes[uid] = (args, kwargs)
        return synthetic_tuple(('document', uid,))

    processor_nodes = {}
    def create_synthetic_processor(name: str):
        def synthetic_processor(*args, **kwargs):
            uid = uuid.uuid4().hex
            node = (name, uid)
            for i, a in enumerate(args):
                if isinstance(a, synthetic_tuple):
                    a = a.tuple()
                Flow.flow_ref().put_edge(a, node, i)
            processor_nodes[uid] = (name, args, kwargs)
            return synthetic_tuple((name, uid,))
        return synthetic_processor

    globals_, locals_ = state

    locals_['collection'] = synthetic_collection
    locals_['document'] = synthetic_document
    locals_['table'] = synthetic_table


    class Synth(ast.NodeVisitor):
        def visit_Call(self, node):
            if isinstance(node.func, ast.Name):
                if node.func.id not in locals_:
                    locals_[node.func.id] = create_synthetic_processor(node.func.id)
            self.generic_visit(node)

    synth = Synth()
    return_value = None

    with Flow() as flow:
        for stmt in stmts:
            synth.visit(stmt)
            if isinstance(stmt, ast.Return):
                value = compile(ast.Expression(body=stmt.value), '<string>', 'eval')
                return_value = eval(value, globals_, locals_)
            elif isinstance(stmt, ast.Assign):
                if not isinstance(stmt.value, ast.Call):
                    raise RuntimeError(
                        "You are not allowed to assign to variables "
                        "to non-call expressions."
                    )
                call_value = eval(compile(ast.Expression(body=stmt.value), '<string>', 'eval'), globals_, locals_)
                assert isinstance(call_value, synthetic_tuple)

                for target in stmt.targets:
                    if isinstance(target, ast.Name):
                        if target.id not in targets:
                            targets[target.id] = [call_value]
                        else:
                            targets[target.id].append(call_value.tuple())
                exec(f"{stmt.targets[0].id} = {call_value!r}", globals_, locals_)

            elif isinstance(stmt, (ast.If, ast.While)):
                raise RuntimeError(
                    "You are not allowed to use 'if' or 'while' statements."
                )
            else:
                try:
                    code = ast.unparse(stmt)
                    exec(compile(ast.Module(body=[stmt], type_ignores=[]), 'flow.py', 'exec'), globals_, locals_)
                except Exception as e:
                    # Create a new exception with the desired information
                    raise RuntimeError(
                        f"Could not execute the code in the flow.\n"
                        f">>> {type(e).__name__}: {e.args[0]} in line\n"
                        f">>> {code}\n"
                    ) from None



    return flow, return_value, (collection_args, document_nodes, processor_nodes), targets






def boot_flow(
    function: Callable,
    __globals: dict,
    __locals: dict,
    /,
    *function_args,
    **function_kwargs,
):
    arguments = inspect.signature(function).parameters

    for key, p in enumerate(arguments.values()):
        if key not in __locals:
            __locals[key] = p.default

    # Map positional only
    for i, param in enumerate(arguments.values()):
        if param.kind == param.POSITIONAL_ONLY:
            __locals[param.name] = function_args[i]

    # Map rest of the positional arguments
    for i, param in enumerate(arguments.values()):
        if param.kind == param.POSITIONAL_OR_KEYWORD and i < len(function_args):
            __locals[param.name] = function_args[i]

    # Map varargs
    varargs_ = ()
    name = None
    for i, param in enumerate(arguments.values()):
        if param.kind == param.VAR_POSITIONAL:
            name = param.name
            varargs_ = function_args[i:]
            break

    if name:
        __locals[name] = varargs_
    # Map keywords
    for key, value in function_kwargs.items():
        if key in __locals:
            __locals[key] = value

    # Check __locals to see if all arguments are mapped
    for key, p in arguments.items():
        if key not in __locals:
            raise TypeError(f"Missing required argument: {key}")

    func_body = inspect.getsource(function)

    indent_ = re.search(
        r'^(?P<INDENT>\s*)def', func_body,
        flags=re.MULTILINE
    ).group('INDENT')

    func_body = re.sub(rf'^{indent_}', '', func_body, flags=re.MULTILINE)

    body = ast.parse(func_body).body[0].body
    return_value, _, return_map = exec_flow(
        body, (__globals, __locals), inspect.getsourcefile(function)
    )

    # return_map.append((None, return_value))

    return return_map
