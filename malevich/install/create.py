from typing import Iterable


def create_processor(name: str, *args: Iterable[tuple[str, bool]]) -> str:
    args_str = ", ".join(
        [f"{arg[0]}" if not arg[1] else f"{arg[0]}: list" for arg in args]
    )

    function_signature = f"def {name} ({args_str}):"
    function_body = "\n".join(
        [f'\t{arg[0]}._autoflow.calledby("{name}")' for arg in args if not arg[1]]
    )
    if function_body:
        function_body += "\n"
    function_body += "\n".join(
        [
            f"\tfor arg in {args[0]}:" f'\n\t\targ._autoflow.calledby("{name}")'
            for arg in args
            if arg[1]
        ]
    )

    return "\n".join([function_signature, function_body])
