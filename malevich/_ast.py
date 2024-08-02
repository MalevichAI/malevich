import ast
import inspect
import re
from copy import deepcopy
from typing import Callable
import warnings

from malevich._autoflow.flow import Flow
from malevich._autoflow.tracer import traced
from malevich._autoflow.tree import ExecutionTree
from .models.nodes.joint import JointNode
from .models.nodes.base import BaseNode
from malevich.models.nodes.operation import OperationNode
from malevich.models.exceptions import NewVariableInBranchError

def copy_state(state):
    return state[0], deepcopy(state[1])


def extract_conditioned_nodes(old_tree: ExecutionTree, new_tree: ExecutionTree):
    old_tree_nodes = set(old_tree.nodes())
    new_tree_nodes = set(new_tree.nodes())
    new_nodes = new_tree_nodes.difference(old_tree_nodes)

    for new_node in new_nodes:
        for from_node, _, _ in new_tree.edges_to(new_node):
            if from_node not in old_tree_nodes:
                break
        else: yield new_node 

        
def validate_branch_states(
    before_branch,
    if_state,
    if_body: list[ast.stmt],
    else_state = None ,
    else_body: list[ast.stmt] = None,
) -> NewVariableInBranchError | None:
    for key in before_branch[1]:
        if if_state[1][key] is not before_branch[1][key]:
            pass
        
    for key in if_state[1]:
        if key not in before_branch[1]:
            return prep_synerr_assign(key, if_body)

    if else_state:
        for key in else_state[1]:
            if key not in before_branch[1]:
                return prep_synerr_assign(key, else_body)
    
        
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
                                details=('<string>', stmt.lineno, target.col_offset, ast.unparse(node))
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
    st, ifst, elst = state[1], if_state[1], else_state[1]
    for key in st:
        if key in ifst and key in elst and (st[key] is not ifst[key] or st[key] is not elst[key]):  # noqa: E501
            v, ifv, elsev = st[key], ifst[key], elst[key]
            if (
                all(map(lambda x: isinstance(x, traced), (v, ifv, elsev)))
                and all(map(lambda x: isinstance(x.owner, BaseNode), (v, ifv, elsev)))
            ):
                st[key].claim(JointNode(nodes=[
                    (v.owner, cond_node, None,),
                    (ifv.owner, cond_node, True,),
                    (elsev.owner, cond_node, False,)
                ]))



def exec_flow(
    stmts: list[ast.stmt],
    state,
    filename: str | None = None,
    function_name: str | None = None
):
    for stmt in stmts:
        if isinstance(stmt, ast.Return):
            value = ast.Expression(body=stmt.value)
            return_value = eval(compile(value, '<string>', 'eval'), state[0], state[1])
            # That is safe to return as a I consider only one level
            return return_value, state
        elif isinstance(stmt, ast.If):
            if_expr = ast.Expression(body=stmt.test)

            if_expr_value = eval(
                compile(if_expr, '<string>', 'eval'),
                state[0],
                state[1]
            )

            if not isinstance(if_expr_value, traced) or not isinstance(if_expr_value.owner, OperationNode):
                raise SyntaxError(
                    "Condition expression within the flow can only "
                    "be a @condition functions and their boolean combinations. "
                    f"The expression evaluated to `{type(if_expr_value).__name__}`.",
                    (filename, 0, stmt.col_offset, 'if '+ ast.unparse(stmt.test))
                )

                
            if_tree = deepcopy(Flow.flow_ref())
            cstate = retrace(if_tree, copy_state(state))

            if_return, if_state = exec_flow(
                stmt.body,
                cstate,
                filename=filename
            )
            
            if stmt.orelse:
                else_tree = deepcopy(Flow.flow_ref())
                cstate = retrace(else_tree, copy_state(state))

                else_return, else_state = exec_flow(
                    stmt.orelse,
                    cstate,
                    filename=filename
                )
            else:
                else_return, else_state = None, None

            # Compare same names
            # state[overlap_name] = my_model(if_state.get, else_state.get)
                
            if if_return is not None or else_return is not None:
                warnings.warn(
                    "Malevich not yet inteprets the return statement within the branches. "
                )
                
            if (exc := validate_branch_states(state, if_state, stmt.body, else_state, stmt.orelse)):
                raise exc.finalize(filename, function_name)
            
            for node in extract_conditioned_nodes(Flow.flow_ref(), if_tree):
                if isinstance(node, traced) and isinstance(node.owner, OperationNode):
                    node.owner.should_be_true.append(if_expr_value.owner.uuid)
                
            for node in extract_conditioned_nodes(Flow.flow_ref(), else_tree):
                if isinstance(node, traced) and isinstance(node.owner, OperationNode):
                    node.owner.should_be_false.append(if_expr_value.owner.uuid)
                    
            merge_detached_tree_inplace(Flow.flow_ref(), if_tree)
            merge_detached_tree_inplace(Flow.flow_ref(), else_tree)
            
            state = retrace(Flow.flow_ref(), state)
            if else_state:
                combine_branch_states_inplace(state, if_state, else_state, if_expr_value.owner)
        else:
            exec(compile(ast.Module(body=[stmt], type_ignores=[]), '<string>', 'exec'), state[0], state[1])

    return None, state

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
    indent_ = re.search(r'^(?P<INDENT> *)def', func_body, flags=re.MULTILINE).group('INDENT')
    func_body = re.sub(rf'^{indent_}', '', func_body, flags=re.MULTILINE)

    body = ast.parse(func_body).body[0].body
    return_value, _ = exec_flow(
        body, (__globals, __locals), inspect.getsourcefile(function)
    )

    return return_value, 
