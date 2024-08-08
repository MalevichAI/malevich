from functools import partial, wraps

import malevich_coretools as api

from malevich._utility.core_logging import IgnoreCoreLogs

from ..refs import BaseRef
from .base import BaseCoreService


def update_doc_by_name(name, *args, **kwargs):
    with IgnoreCoreLogs():
        return api.update_doc(
            id=api.get_doc_by_name(
                name=name,
                auth=kwargs["auth"],
                conn_url=kwargs["conn_url"]
            ).id,
            *args,
            **kwargs
        )

def delete_doc_by_name(name, *args, **kwargs):
    return api.delete_doc(
        id=api.get_doc_by_name(
            name=name,
            auth=kwargs["auth"],
            conn_url=kwargs["conn_url"]
        ).id,
        *args,
        **kwargs
    )

class DocumentService(BaseCoreService):
    def __init__(self, auth: api.AUTH, conn_url: str) -> None:
        super().__init__(auth, conn_url)

    def id(
        self,
        id: str,
        /,
    ):
        return BaseRef(
            name=f'DocumentByIdRef({id})',
            create=None,
            delete=partial(
                api.delete_doc,
                id, auth=self.auth, conn_url=self.conn_url
            ),
            update=partial(
                api.update_doc,
                id, auth=self.auth, conn_url=self.conn_url
            ),
            get=partial(
                api.get_doc,
                id, auth=self.auth, conn_url=self.conn_url
            ),
            list=None,
        )


    def name(
        self,
        name: str,
        /,
    ):
        return BaseRef(
            name=f'DocumentByNameRef({name})',
            create=partial(
                api.create_doc,
                name=name, auth=self.auth, conn_url=self.conn_url
            ),
            delete=partial(
                delete_doc_by_name,
                name,
                auth=self.auth,
                conn_url=self.conn_url
            ),
            update=partial(
                update_doc_by_name,
                name,
                auth=self.auth,
                conn_url=self.conn_url
            ),
            get=partial(
                api.get_doc_by_name,
                name, auth=self.auth, conn_url=self.conn_url
            ),
            list=None
        )

    def all(self):
        return BaseRef(
            'DocumentAllRef',
            create=None,
            delete=partial(
                api.delete_docs,
                auth=self.auth, conn_url=self.conn_url
            ),
            update=None,
            get=None,
            list=partial(
                api.get_docs,
                auth=self.auth, conn_url=self.conn_url
            )
        )



