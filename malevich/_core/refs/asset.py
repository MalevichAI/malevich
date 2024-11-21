from typing import Generic, TypeVar

from .base import BaseRef

CreateFn = TypeVar('CreateFn', bound=callable)
DeleteFn = TypeVar('DeleteFn', bound=callable)
UpdateFn = TypeVar('UpdateFn', bound=callable)
GetFn = TypeVar('GetFn', bound=callable)
ListFn = TypeVar('ListFn', bound=callable)
GetLinkFn = TypeVar('GetLinkFn', bound=callable)
PostLinkFn = TypeVar('PostLinkFn', bound=callable)

class AssetRef(
    Generic[CreateFn, DeleteFn, UpdateFn, GetFn, ListFn, GetLinkFn, PostLinkFn],
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
        get_link: GetLinkFn | None = None,
        post_link: PostLinkFn | None = None,
    ) -> None:
        super().__init__(name, create, delete, update, get, list)
        self.get_link = get_link
        self.post_link = post_link
