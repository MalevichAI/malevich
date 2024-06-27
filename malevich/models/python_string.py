from typing import Annotated

from pydantic import AfterValidator


def _pystr_validate(_str: str) -> str:
    if not isinstance(_str, str):
        raise ValueError('Invalid Python variable name. Expected a string')
    if not _str.isidentifier():
        raise ValueError(
            'Invalid Python variable name. Must be a valid Python identifier'
        )
    return _str

PythonString = Annotated[str, AfterValidator(_pystr_validate)]
