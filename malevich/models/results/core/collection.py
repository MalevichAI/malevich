from functools import cache

import malevich_coretools as core
import pandas as pd

from ..base import BaseResult


class CoreCollectionResult(BaseResult[list[pd.DataFrame]]):
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
    def get(self) -> list[pd.DataFrame]:
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

        return results


class CoreLocalCollectionResult(BaseResult[list[pd.DataFrame]]):
    def __init__(self, dataframes: list[pd.DataFrame]) -> None:
        super().__init__()
        self._dfs = dataframes

    def get(self) -> list[pd.DataFrame]:
        return self._dfs
