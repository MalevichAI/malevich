from typing import Iterable

imports = """
from uuid import uuid4
from malevich._autoflow.manager import Registry
from malevich._autoflow.tracer import TracedAppData, tracer
"""

register = """
Registry().register("{app_name}__image_ref", {image_ref})
Registry().register("{app_name}__image_auth_user", {image_auth_user})
Registry().register("{app_name}__image_auth_token", {image_auth_token})
"""

processor = """
def {app_name}({args}, config=dict()):  # noqa: ANN003, ANN002, ANN204
   Registry().register("{app_name}_", ()}
"""

single_arg = """
    {arg_name}._autoflow.calledby(traced_app_data)
"""

iterable_arg = """
    for arg in {arg_name}:
        arg._autoflow.calledby(traced_app_data)
"""

return_statement = """
    return tracer(traced_app_data)
"""


def create_processors(
    names: list[str],
    image_ref: str,
    image_auth: tuple[str, str],
    args: Iterable[Iterable[tuple[str, bool]]],
) -> str:
    metascript = imports
    for name, _args in zip(names, args):
        metascript += register.format(
            app_name=name,
            image_ref=image_ref,
            image_auth_user=image_auth[0],
            image_auth_token=image_auth[1],
        )
        args_str = [(f"{arg[0]}: list" if arg[1] else f"{arg[0]}") for arg in _args]
        metascript += processor.format(app_name=name, args=", ".join(args_str))
        for arg in args:
            if arg[1]:
                metascript += iterable_arg.format(arg_name=arg[0])
            else:
                metascript += single_arg.format(arg_name=arg[0])

        metascript += return_statement
        metascript += "\n\n"
    return metascript
