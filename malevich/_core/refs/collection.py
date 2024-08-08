from typing import Generic, TypeVar

from .base import BaseRef

CreateFn = TypeVar('CreateFn', bound=callable)
DeleteFn = TypeVar('DeleteFn', bound=callable)
UpdateFn = TypeVar('UpdateFn', bound=callable)
GetFn = TypeVar('GetFn', bound=callable)
GetTableFn = TypeVar('GetTableFn', bound=callable)
ListFn = TypeVar('ListFn', bound=callable)
ListAllFn = TypeVar('ListAllFn', bound=callable)

class CollectionRef(
    Generic[CreateFn, DeleteFn, UpdateFn, GetFn, ListFn, GetTableFn],
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
        get_table: GetTableFn | None = None,
    ) -> None:
        super().__init__(name, create, delete, update, get, list)
        self.get_table = get_table
