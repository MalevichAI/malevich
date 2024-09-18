import inspect
from functools import wraps
import os
from typing import TypeVar

T = TypeVar('T')

def capture_caller_file(function: T) -> T:
    @wraps(function)
    def wrapped_function(*args, **kwargs):
        f_back_g = inspect.currentframe().f_back.f_globals
        if '__file__' in f_back_g:
            caller_file = f_back_g['__file__']
            if caller_file and caller_file.endswith('.py'):
               from malevich._analytics import manager
               manager.write_artifact({
                   'type': 'file_content',
                   'file': os.path.abspath(caller_file),
                   'content': open(caller_file).read(),
                   'writer': function.__qualname__
               })

        return function(*args, **kwargs)
    return wrapped_function
