from functools import cache

import malevich_coretools as core
import pandas as pd

from ..base import BaseResult


class CoreAssetBody:
    count: int
    paths: list[str]
    data: list[bytes]

    def __init__(self) -> None:
        self.count = 0
        self.paths = []
        self.data = []


class CoreAssetResult(BaseResult[list[CoreAssetBody]]):
    """A result of operation that returns a collection"""

    def __init__(
        self,
        core_group_name: str,
        core_operation_id: str,
        conn_url: str,
        auth: core.AUTH,
    ) -> None:
        self.core_group_name = core_group_name
        self._conn_url = conn_url
        self._auth = auth
        self.core_operation_id = core_operation_id

    @cache
    def get(self) -> list[CoreAssetBody]:
        results = []
        collection_ids = [
            x.id for x in core.get_collections_by_group_name(
                self.core_group_name,
                operation_id=self.core_operation_id,
                auth=self._auth,
                conn_url=self._conn_url
            ).data
        ]

        results.extend([
            core.get_collection_to_df(
                i,
                auth=self._auth,
                conn_url=self._conn_url
            )
            for i in collection_ids
        ])

        asset_results = []

        for df_ in results:
            if not isinstance(df_, pd.DataFrame):
                pass

            if 'path' not in df_.columns:
                pass

            obj_path = df_.path.iloc[0]
            assert isinstance(obj_path, str), \
                f"Object path must be a string, but got {type(obj_path)}"

            body_ = CoreAssetBody()
            objects_ = core.get_collection_objects(
                obj_path,
                auth=self._auth,
                conn_url=self._conn_url,
                recursive=True
            )
            for file_ in objects_.files:
                body_.count += 1
                object_ = core.get_collection_object(
                    obj_path.rstrip("/") + "/" + file_.lstrip("/"),
                    auth=self._auth,
                    conn_url=self._conn_url,
                )
                body_.paths.append(file_)
                body_.data.append(object_)
            asset_results.append(body_)

        return asset_results
