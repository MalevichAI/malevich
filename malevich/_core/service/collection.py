from functools import partial, wraps
from typing import TypeVar

import malevich_coretools as api

from ..._utility.core_logging import IgnoreCoreLogs
from ..refs import BaseRef, CollectionRef
from .base import BaseCoreService

T = TypeVar("T")

def map_name_to_id(fn: T, pass_name: bool = False) -> T:
    def wrapper(name, *args, **kwargs):
        with IgnoreCoreLogs():
            if pass_name:
                return fn(
                    id=api.get_collection_by_name(
                        name=name,
                        auth=kwargs["auth"],
                        conn_url=kwargs["conn_url"]
                    ).id,
                    name=name,
                    *args,
                    **kwargs
                )

            return fn(
                id=api.get_collection_by_name(
                    name=name,
                    auth=kwargs["auth"],
                    conn_url=kwargs["conn_url"]
                ).id,
                *args,
                **kwargs
            )
    return wrapper

def update_collection_from_df(id, data, *args, **kwargs):
    docs = [
        api.create_doc(
            data=x,
            name=None,
            auth=kwargs["auth"],
            conn_url=kwargs["conn_url"]
        )
        for x in data.to_dict(orient="records")
    ]
    return api.update_collection(
        id=id,
        ids=docs,
        *args,
        **kwargs
    )

class CollectionService(BaseCoreService):
    def __init__(self, auth: api.AUTH, conn_url: str) -> None:
        super().__init__(auth, conn_url)

    def id(
        self,
        id: str,
        /,
    ):
        return CollectionRef(
            name=f'CollectionByIdRef({id})',
            create=None,
            delete=partial(
                api.delete_collection,
                id, auth=self.auth, conn_url=self.conn_url
            ),
            update=partial(
                update_collection_from_df,
                id, auth=self.auth, conn_url=self.conn_url
            ),
            get=partial(
                api.get_collection,
                id, auth=self.auth, conn_url=self.conn_url
            ),
            list=None,
            get_table=partial(
                api.get_collection_to_df,
                id, auth=self.auth, conn_url=self.conn_url
            ),
        )


    def name(
        self,
        name: str,
        /,
    ):
        return CollectionRef(
            name=f'CollectionByNameRef({name})',
            create=partial(
                api.create_collection_from_df,
                name=name, auth=self.auth, conn_url=self.conn_url
            ),
            update=partial(
                map_name_to_id(update_collection_from_df, pass_name=True),
                name=name, auth=self.auth, conn_url=self.conn_url
            ),
            delete=partial(
                map_name_to_id(api.delete_collection),
                name=name, auth=self.auth, conn_url=self.conn_url
            ),
            get=partial(
                api.get_collection_by_name,
                name, auth=self.auth, conn_url=self.conn_url
            ),
            get_table=partial(
                api.get_collection_by_name_to_df,
                name, auth=self.auth, conn_url=self.conn_url
            ),
            list=partial(
                api.get_collections_by_name,
                name, auth=self.auth, conn_url=self.conn_url
            ),
        )

    def group_name(
        self,
        group_name: str,
        /,
    ):
        return BaseRef(
            name=f'CollectionsByGroupRef({group_name})',
            create=None,
            delete=None,
            update=None,
            get=None,
            list=partial(
                api.get_collections_by_group_name,
                group_name,
                auth=self.auth,
                conn_url=self.conn_url
            )
        )


    def all(self):
        return BaseRef(
            name='AllCollectionsRef',
            create=None,
            delete=partial(
                api.delete_collections,
                auth=self.auth, conn_url=self.conn_url
            ),
            update=None,
            get=None,
            list=partial(
                api.get_collections,
                auth=self.auth, conn_url=self.conn_url
            )
        )



