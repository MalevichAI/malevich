from functools import partial, wraps
from inspect import Parameter, signature

from typer.models import CommandInfo


def make_command(core_op, *args, callback=None, **kwargs) -> CommandInfo:
    fn = partial(core_op, auth=None, conn_url=None, batcher=None)

    @wraps(callback)
    def wrapped(*args, **kwargs) -> None:
        results = fn(*args, **kwargs)
        if callback:
            callback(results)
        else:
            print(results)

    # set wrapped args and kwargs to
    # fn's signature
    annotations = fn.func.__annotations__
    for k in fn.keywords:
        annotations.pop(k, None)
    annotations.pop('return', None)
    sig = signature(fn)
    sig = sig.replace(parameters=[
        Parameter(name=k, kind=Parameter.POSITIONAL_OR_KEYWORD, annotation=v)
        for k, v in annotations.items()
    ])
    wrapped.__annotations__ = annotations
    wrapped.__signature__ = sig
    command = CommandInfo(
        *args,
        callback=wrapped,
        **kwargs,
    )
    return command


def wrap_command(core_f, exclude: list[str] | None = None):
    if not exclude:
        exclude = []

    fn = partial(core_f, auth=None, conn_url=None, batcher=None)

    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs) -> None:
            return f(*args, **kwargs)

        # set wrapped args and kwargs to
        # fn's signature
        annotations = fn.func.__annotations__
        for k in fn.keywords:
            annotations.pop(k, None)
        annotations.pop('return', None)
        fn_sig = signature(fn)
        fn_sig = fn_sig.replace(parameters=[
            x for x in fn_sig.parameters.values()
            if x.name in annotations
        ])
        f_signature = signature(f)
        # merge annotations
        wrapped.__annotations__ = {**annotations, **f.__annotations__}
        wrapped.__annotations__.pop('kwargs', None)

        params = [
            *fn_sig.parameters.values(), *f_signature.parameters.values()
        ]
        names = {'kwargs', *exclude}
        unique_params: list[Parameter] = []
        for p in params:
            if p.name not in names:
                unique_params.append(p)
            names.add(p.name)
        unique_params.sort(key=lambda x: x.default != Parameter.empty)
        # merge signatures
        f_signature = f_signature.replace(
            parameters=unique_params
        )

        wrapped.__signature__ = f_signature
        return wrapped

    return decorator
