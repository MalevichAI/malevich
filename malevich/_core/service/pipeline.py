from functools import partial, wraps

import malevich_coretools as api

from ..refs import BaseRef, PRSRef
from .service import BaseCoreService


def _get_real_id(s: api.ResultIdsMap, id):
    for k in s.ids:
        if k.id == id:
            return k.realId
    return None

def map_real_pipeline_id(fn):
    @wraps(fn)
    def wrapper(id, *args, **kwargs):
        real_id = None
        return fn(
            id=real_id or (real_id := _get_real_id(api.get_pipelines_map(), id)),
            *args,
            **kwargs
        )
    return wrapper

class PipelineService(BaseCoreService):
    def __init__(self, auth: api.AUTH, conn_url: str) -> None:
        super().__init__(auth, conn_url)

    def id(
        self,
        id: str,
        /,
    ):
        return PRSRef(
            f'PipelineByIdRef({id})',
            create=partial(
                api.create_pipeline,
                pipeline_id=id, auth=self.auth, conn_url=self.conn_url
            ),
            delete=partial(
                map_real_pipeline_id(api.delete_pipeline),
                id, auth=self.auth, conn_url=self.conn_url
            ),
            update=partial(
                map_real_pipeline_id(api.update_pipeline),
                id=id, # mapped to real id
                pipeline_id=id,
                auth=self.auth,
                conn_url=self.conn_url
            ),
            get=partial(
                api.get_pipeline,
                id,
                auth=self.auth,
                conn_url=self.conn_url
            ),
            prepare=partial(
                api.pipeline_prepare,
                pipeline_id=id,
                auth=self.auth,
                conn_url=self.conn_url
            ),
            list=None,
        )


    def all(
        self,
    ):
        return BaseRef(
            'PipelineAllRef',
            create=None,
            delete=partial(
                api.delete_pipelines,
                auth=self.auth, conn_url=self.conn_url
            ),
            update=None,
            get=None,
            list=partial(
                api.get_pipelines,
                auth=self.auth, conn_url=self.conn_url
            ),
        )
