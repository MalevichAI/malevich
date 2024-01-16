from functools import cache
from typing import Optional

import malevich_coretools as core
import pandas as pd

from ..base import BaseResult


class CoreResultPayload:
    """An actual result information

    DO NOT USE THIS CLASS DIRECTLY
    """

    def __init__(
        self,
        data: pd.DataFrame | list[bytes],
        is_asset: bool = False,
        is_composite_asset: bool = False,
        is_collection: bool = False,
        paths: Optional[list[str]] = None,
    ) -> None:
        assert not (is_asset and is_collection), \
            "Result can't be both asset and collection"
        if is_composite_asset:
            assert is_asset, \
                "Composite asset must be an asset"

        self._data = data
        self._is_asset = is_asset
        self._is_composite_asset = is_composite_asset
        self._is_collection = is_collection
        self._paths = paths or []

    def is_asset(self) -> bool:
        return self._is_asset

    def is_composite_asset(self) -> bool:
        return self._is_composite_asset

    def is_collection(self) -> bool:
        return self._is_collection

    def count_objects(self) -> int:
        if self._is_composite_asset:
            return len(self._paths)
        if self._is_asset:
            return 1
        if self._is_collection:
            return len(self._data.index)

    @property
    def data(self) -> pd.DataFrame | dict[str, bytes] | bytes:
        if self._is_asset:
            if self._is_composite_asset:
                return {
                    path: self._data[i]
                    for i, path in enumerate(self._paths)
                }
            else:
                return self._data[0]
        else:
            return self._data

    def __str__(self) -> str:
        return (
            f"CoreResultPayload(is_asset={self._is_asset}, "
            f"is_collection={self._is_collection} "
            f"is_composite_asset={self._is_composite_asset} "
            f"paths={self._paths})"
        )

    def __repr__(self) -> str:
        return (
            f"CoreResultPayload(is_asset={self._is_asset}, "
            f"is_collection={self._is_collection} "
            f"is_composite_asset={self._is_composite_asset} "
            f"paths={self._paths})"
        )

class CoreResult(BaseResult[CoreResultPayload]):
    """A result of operation that returns a collection"""

    @staticmethod
    def is_asset(data: pd.DataFrame) -> bool:
        return data.shape == (1, 1) and data.columns[0] == "path"

    @staticmethod
    def extract_path_to_asset(path: str, user: str) -> str:
        return path.split(f"/mnt_obj/{user}/")[-1]

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

    def __str__(self) -> str:
        return f'CoreResult(core_operation_id="{self.core_operation_id}")'

    def __repr__(self) -> str:
        return f'CoreResult(core_operation_id="{self.core_operation_id}")'

    @cache
    def get(self) -> list[CoreResultPayload]:
        results_: list[pd.DataFrame] = []
        collection_ids = [
            x.id for x in core.get_collections_by_group_name(
                self.core_group_name,
                operation_id=self.core_operation_id,
                auth=self._auth,
                conn_url=self._conn_url
            ).data
        ]

        results_.extend([
            core.get_collection_to_df(
                i,
                auth=self._auth,
                conn_url=self._conn_url
            )
            for i in collection_ids
        ])

        results = []
        for result in results_:
            if CoreResult.is_asset(result):
                obj_path = CoreResult.extract_path_to_asset(
                    result.path.iloc[0],
                    user=self._auth[0],
                )
                try:
                    objects_ = core.get_collection_objects(
                        obj_path,
                        auth=self._auth,
                        conn_url=self._conn_url,
                        recursive=True
                    )
                    if len(objects_.files) == 1:
                        object_ = core.get_collection_object(
                            obj_path + "/" + objects_.files[0],
                            auth=self._auth,
                            conn_url=self._conn_url,
                        )
                        results.append(CoreResultPayload(
                            data=[object_],
                            is_asset=True,
                        ))
                    else:
                        paths = []
                        obj_bytes = []
                        for file_ in objects_.files:
                            object_ = core.get_collection_object(
                                obj_path.rstrip("/") + "/" + file_.lstrip("/"),
                                auth=self._auth,
                                conn_url=self._conn_url,
                            )
                            paths.append(file_)
                            obj_bytes.append(object_)
                        results.append(CoreResultPayload(
                            data=obj_bytes,
                            is_asset=True,
                            is_composite_asset=True,
                            paths=paths,
                        ))
                except Exception as _:
                    try:
                        object_ = core.get_collection_object(
                            obj_path,
                            auth=self._auth,
                            conn_url=self._conn_url,
                        )
                        results.append(CoreResultPayload(
                            data=[object_],
                            is_asset=True,
                        ))
                    except Exception as _:
                        results.append(CoreResultPayload(
                            data=result,
                            is_collection=True,
                        ))
            else:
                results.append(CoreResultPayload(
                    data=result,
                    is_collection=True,
                ))

        return results


class CoreLocalDFResult(BaseResult[pd.DataFrame]):
    def __init__(
       self,
       df: pd.DataFrame,
    ) -> None:
        self._df = df

    def get(self) -> pd.DataFrame:
        return self._df
