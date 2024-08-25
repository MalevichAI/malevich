import sys
from contextlib import redirect_stderr, redirect_stdout
from functools import partial, wraps
from inspect import isfunction
from io import StringIO
from typing import TypeVar

from requests.exceptions import HTTPError

from malevich._analytics import manager

T = TypeVar('T')

def _func_name(obj):
    if isfunction(obj):
        return obj.__name__
    elif isinstance(obj, partial):
        return obj.func.__name__
    else:
        return obj.__class__.__name__

def capture_artifact(function: T) -> T:
    @wraps(function)
    def artifact_write(*args, **kwargs):
        artifact = {
            'type': 'core_api_call',
            'query': _func_name(function),
            'args': list(args) + (
                list(function.args) if isinstance(function, partial) else []),
            'kwargs': {**kwargs,
                **(function.keywords if isinstance(function, partial) else {})
            },
        }
        stdout_ = StringIO()
        stderr_ = StringIO()
        try:
            with redirect_stdout(stdout_), redirect_stderr(stderr_):
                artifact['result'] = function(*args, **kwargs)
        except HTTPError as e:
            artifact['error'] = str(e)
        finally:
            artifact['stdout'] = stdout_.getvalue()
            artifact['stderr'] = stderr_.getvalue()

        stdout_.close()
        stderr_.close()

        sys.stdout.write(artifact['stdout'])
        sys.stderr.write(artifact['stderr'])
        manager.write_artifact(artifact)
        return artifact['result']

    return artifact_write
