import ast
import os

input_functions = ["input_doc", "input_df", "input_true"]


class SearchDecoratorsVisitor(ast.NodeVisitor):
    decorated_functions = []


class ProcessorSearchVisitor(SearchDecoratorsVisitor):
    """Find all calls to `processor()` function decorator."""

    def __init__(self) -> None:
        """Initialize the visitor."""
        self.decorated_functions = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        """Visit all function definitions."""
        # Iterate over all decorators of the function
        for decorator in node.decorator_list:
            # Check if the decorator is a call
            if isinstance(decorator, ast.Call):
                if (
                    # HACK: Assume that the decorator is a call to `processor` function
                    isinstance(decorator.func, ast.Attribute)
                    and decorator.func.attr == "processor"
                ):
                    self.decorated_functions.append(node.name)
        self.generic_visit(node)


class InputSearchVisitor(SearchDecoratorsVisitor):
    """Find all calls to `input_*()` function decorators."""

    def __init__(self) -> None:
        self.decorated_functions = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        # Iterate over all decorators of the function
        for decorator in node.decorator_list:
            # Check if the decorator is a call
            if isinstance(decorator, ast.Call):
                if (
                    # HACK: Assume that the decorator is a call to `input_*` function
                    isinstance(decorator.func, ast.Attribute)
                    and decorator.func.attr in input_functions
                ):
                    self.decorated_functions.append(node.name)
        self.generic_visit(node)


class InitSearchVisitor(SearchDecoratorsVisitor):
    """Find all calls to `init()` function decorator."""

    def __init__(self) -> None:
        self.decorated_functions = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        # Iterate over all decorators of the function
        for decorator in node.decorator_list:
            # Check if the decorator is a call
            if isinstance(decorator, ast.Call):
                if (
                    # HACK: Assume that the decorator is a call to `init` function
                    isinstance(decorator.func, ast.Attribute)
                    and decorator.func.attr == "init"
                ):
                    self.decorated_functions.append(node.name)
        self.generic_visit(node)


class OutputSearchVisitor(SearchDecoratorsVisitor):
    """Find all calls to `output()` function decorator."""

    def __init__(self) -> None:
        self.decorated_functions = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        # Iterate over all decorators of the function
        for decorator in node.decorator_list:
            # Check if the decorator is a call
            if isinstance(decorator, ast.Call):
                if (
                    # HACK: Assume that the decorator is a call to `output` function
                    isinstance(decorator.func, ast.Attribute)
                    and decorator.func.attr == "output"
                ):
                    self.decorated_functions.append(node.name)
        self.generic_visit(node)


def _scan_tree(
    file: os.PathLike,
    visitor: SearchDecoratorsVisitor,
    skip_syntax_errors: bool = True,
) -> list[str]:
    with open(file) as f:
        try:
            tree = ast.parse(f.read())
        except SyntaxError:
            if skip_syntax_errors:
                return []
            else:
                raise

    visitor.visit(tree)
    return visitor.decorated_functions


def scan_processors(file: os.PathLike) -> list[str]:
    # Traverse the python file and find
    # all calls to `processor()`
    # function decorator

    return _scan_tree(file, ProcessorSearchVisitor())


def scan_inputs(file: os.PathLike) -> list[str]:
    # Traverse the python file and find
    # all calls to `processor()`
    # function decorator

    return _scan_tree(file, InputSearchVisitor())


def scan_outputs(file: os.PathLike) -> list[str]:
    # Traverse the python file and find
    # all calls to `processor()`
    # function decorator

    return _scan_tree(file, OutputSearchVisitor())


def scan_inits(file: os.PathLike) -> list[str]:
    # Traverse the python file and find
    # all calls to `processor()`
    # function decorator

    return _scan_tree(file, InitSearchVisitor())


def scan(file: os.PathLike) -> list[str]:
    # Traverse the python file and find
    # all calls to `processor()`
    # function decorator

    return (
        scan_processors(file),
        scan_inputs(file),
        scan_outputs(file),
        scan_inits(file),
    )
