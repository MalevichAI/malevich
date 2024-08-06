from typing import Generic, TypeVar

from ..._utility.core_logging import IgnoreCoreLogs

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
            ref._deleted = False
            return out
        if delete:
            out = fn(*args, **kwargs)
            ref._deleted = True
            ref._created = False
            return out
        return fn(*args, **kwargs)
    return wrapped


def create_if_not_exists(
    ref: 'BaseRef',
    create: CreateFn,
) -> CreateFn:
    def wrapped(*args, **kwargs):
        with IgnoreCoreLogs():
            if (_get := ref._try_get()):
                return _get
        return create(*args, **kwargs)
    return wrapped

def update_or_create(
    ref: 'BaseRef',
    create: CreateFn,
    update: UpdateFn,
) -> UpdateFn:
    def wrapped(*args, **kwargs):
        if ref.update is not None:
            try:
                return update(*args, **kwargs)
            except Exception as e:
                if ref.create is not None:
                    return create(*args, **kwargs)
                else:
                    raise ValueError(
                        f'Failed to update {ref} and '
                        'no create function is available'
                    )
        if ref.create is not None:
            return create(*args, **kwargs)
        else:
            raise ValueError(
                f'No create function is available for {ref}'
            )
    return wrapped

class BaseRef(Generic[CreateFn, DeleteFn, UpdateFn, GetFn, ListFn]):
    def __init__(
        self,
        name: str,
        create: CreateFn | None = None,
        delete: DeleteFn | None = None,
        update: UpdateFn | None = None,
        get: GetFn | None = None,
        list: ListFn | None = None,
    ) -> None:
        self.name = name
        self.create = wrap_fn(create, self, create=True)
        self.delete = wrap_fn(delete, self, delete=True)
        self.update = update
        self.get = get
        self.list = list

        self._created = None
        self._deleted = None

    def _try_get(self):
        if self.get is None:
            return None
        try:
            return self.get() is not None
        except Exception as e:
            return False


    @property
    def created(self) -> None | bool:
        return self._created or self._try_get()


    @property
    def deleted(self) -> None | bool:
        return self._deleted or not self._try_get()

    @property
    def create_if_not_exists(self) -> CreateFn | None:
        return create_if_not_exists(self, self.create)

    @property
    def update_or_create(self) -> UpdateFn | None:
        return update_or_create(self, self.create, self.update)

    def __str__(self) -> str:
        return self.name

