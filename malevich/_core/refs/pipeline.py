from typing import Generic, TypeVar

from .base import BaseRef

CreateFn = TypeVar('CreateFn', bound=callable)
DeleteFn = TypeVar('DeleteFn', bound=callable)
UpdateFn = TypeVar('UpdateFn', bound=callable)
GetFn = TypeVar('GetFn', bound=callable)
PrepareFn = TypeVar('PrepareFn', bound=callable)
RunFn = TypeVar('RunFn', bound=callable)
StopFn = TypeVar('StopFn', bound=callable)
ListFn = TypeVar('ListFn', bound=callable)
ListAllFn = TypeVar('ListAllFn', bound=callable)

class PRSRef(
    Generic[CreateFn, DeleteFn, UpdateFn, GetFn, ListFn, PrepareFn, RunFn, StopFn],
    BaseRef[CreateFn, DeleteFn, UpdateFn, GetFn, ListFn],
):
    def __init__(
        self,
        name: str,
        create: CreateFn | None = None,
        delete: DeleteFn | None = None,
        update: UpdateFn | None = None,
        get: GetFn | None = None,
        list: ListFn | None = None,
        run: RunFn | None = None,
        prepare: PrepareFn | None = None,
        stop: StopFn | None = None,
    ) -> None:
        super().__init__(name, create, delete, update, get, list)
        self.run = run
        self.prepare = prepare
        self.stop = stop

