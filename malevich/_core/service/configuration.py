from functools import partial

import malevich_coretools as api

from ..refs import BaseRef, ConfigRef
from .service import BaseCoreService


class ConfigService(BaseCoreService):
    def __init__(self, auth: api.AUTH, conn_url: str) -> None:
        super().__init__(auth, conn_url)

    def id(
        self,
        id: str,
        /,
    ):
        return BaseRef(
            name=f'ConfigByIdRef({id})',
            create=None,
            delete=partial(
                api.delete_cfg,
                id, auth=self.auth, conn_url=self.conn_url
            ),
            update=partial(
                api.update_cfg,
                id, auth=self.auth, conn_url=self.conn_url
            ),
            get=partial(
                api.get_cfg_real,
                id, auth=self.auth, conn_url=self.conn_url
            ),
            list=None,
        )

    def name(
        self,
        name: str,
        /,
    ):
        return ConfigRef(
            name=f'ConfigByNameRef({name})',
            create=partial(
                api.create_cfg,
                cfg_id=name, auth=self.auth, conn_url=self.conn_url
            ),
            delete=None,
            update=partial(
                api.update_cfg,
                cfg_id=name,
                auth=self.auth,
                conn_url=self.conn_url
            ),
            get=partial(
                api.get_cfg,
                name,
                auth=self.auth,
                conn_url=self.conn_url
            ),
            list=None,
        )

    def all(
        self,
    ):
        return BaseRef(
            name='ConfigAllRef',
            create=None,
            delete=partial(
                api.delete_cfgs,
                auth=self.auth, conn_url=self.conn_url
            ),
            update=None,
            list=partial(
                api.get_cfgs,
                auth=self.auth, conn_url=self.conn_url
            ),
            get=None,
        )
