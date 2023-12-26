import malevich_coretools as mct
from pydantic import BaseModel

from ..._utility.host import fix_host
from ..collection import Collection


class PromisedCollection(BaseModel):
    collection_ref: Collection
    created: bool = False
    core_id: str = None

    def execute_promise(
        self,
        auth: mct.AUTH = None,
        host: str = None,
        force_update: bool = False,
    ) -> None:
        host = fix_host(host)
        if self.collection_ref.persistent:
            try:
                _coll = mct.get_collections_by_name(
                    name=self.collection_ref.magic(),
                    auth=auth,
                    conn_url=host,
                )
                if len(_coll.ownIds) > 0:
                    self.core_id = _coll.ownIds[0]
                if force_update:
                    docs = [
                        row.to_json()
                        for _, row in self.collection_ref.collection_data.iterrows()
                    ]
                    doc_ids = []
                    for doc_ in docs:
                        doc_ids.append(
                            mct.create_doc(
                                data=doc_,
                                auth=auth,
                                collection=self.core_id,
                            )
                        )
                    mct.update_collection(
                        id=self.core_id,
                        ids=doc_ids,
                        auth=auth,
                        conn_url=host,
                    )
                else:
                    
            except
        if not self.created:
            self.core_id = mct.create_collection(
                auth=auth,
                collection=self.collection_ref
            )
            self.created = True
        else:
            raise ValueError("Collection already created")