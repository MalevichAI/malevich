from functools import partial
from typing import Generic, TypeVar

from ..._utility.core_logging import IgnoreCoreLogs

from ..refs.base import BaseRef

CreateFn = TypeVar('CreateFn', bound=callable)
DeleteFn = TypeVar('DeleteFn', bound=callable)
UpdateFn = TypeVar('UpdateFn', bound=callable)
GetFn = TypeVar('GetFn', bound=callable)
ListFn = TypeVar('ListFn', bound=callable)

T = TypeVar('T')
def wrap_fn(
    fn: T,
    ref: 'BaseRef',
    create: bool = False,
    delete: bool = False,
    update: bool = False,
    get: bool = False,
    list: bool = False,
) -> T:

    def wrapped(*args, **kwargs):
        if create:
            out = fn(*args, **kwargs)
            ref._created = True
            return out
        if delete:
            out = fn(*args, **kwargs)
            ref._deleted = True
            return out
        return fn(*args, **kwargs)
    return wrapped


class ConfigRef(BaseRef[CreateFn, DeleteFn, UpdateFn, GetFn, ListFn], 
              Generic[CreateFn, DeleteFn, UpdateFn, GetFn, ListFn]):

    @property
    def update_or_create(self) -> UpdateFn | None:
        try:
            with IgnoreCoreLogs():
                cfg = self.get()
            assert hasattr(cfg, 'id')
        except Exception:
            return self.create
        else:
            return partial(self.update, id=cfg.id)

