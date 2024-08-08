from functools import partial

import malevich_coretools as api

from ..refs import BaseRef, PRSRef
from .base import BaseCoreService


class RunService(BaseCoreService):
    def __init__(self, auth: api.AUTH, conn_url: str) -> None:
        super().__init__(auth, conn_url)

    @property
    def active(self):
        return BaseRef(
            'ActiveRunsRef',
            list=partial(
                api.get_run_active_runs,
                auth=self.auth,
                conn_url=self.conn_url
            ),
        )

    def operation_id(self, operation_id: str):
        return PRSRef(
            'OperationIdRef',
            run=partial(
                api.task_run,
                auth=self.auth,
                conn_url=self.conn_url,
                operation_id=operation_id
            ),
            stop=partial(
                api.task_stop,
                auth=self.auth,
                conn_url=self.conn_url,
                operation_id=operation_id
            ),
        )
