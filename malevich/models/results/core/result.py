import json
import warnings
from functools import cache
from typing import Optional, TypeVar

import malevich_coretools as core
import pandas as pd
from pydantic import BaseModel

from malevich.constants import DEFAULT_CORE_HOST
from malevich.models import Collection

from ..base import BaseResult

DocumentModelType = TypeVar('DocumentModelType', bound=BaseModel)

class CoreResultPayload:
    """An actual information that is saved as result

    A payload can be either asset or collection. If it is an asset,
    it can be a single file or a composite asset (a directory with
    multiple files). It represents one of results of a single operation.

    Payload follows the following convention:

    - If :meth:`is_asset` is True, then :attr:`data` is :class:`bytes` object
    - If :meth:`is_collection` is True, then :attr:`data` is :class:`DataFrame` object
    - If :meth:`is_composite_asset` is True, then :attr:`data` is a dict of :class:`bytes` objects

    """  # noqa: E501

    def __init__(
        self,
        data: pd.DataFrame | dict | list[bytes],
        is_asset: bool = False,
        is_composite_asset: bool = False,
        is_document: bool = False,
        is_collection: bool = False,
        paths: Optional[list[str]] = None,
    ) -> None:
        assert not (is_asset and is_collection), \
            "Result can't be both asset and collection"
        if is_composite_asset:
            assert is_asset, \
                "Composite asset must be an asset"

        self._data = data
        self._is_asset = is_asset or is_composite_asset
        self._is_composite_asset = is_composite_asset
        self._is_collection = is_collection
        self._paths = paths or []
        self.is_document = is_document

    def is_asset(self) -> bool:
        """Checks if the result is an asset

        Returns:
            bool: True if the result is an asset
        """
        return self._is_asset

    def is_composite_asset(self) -> bool:
        """Checks if the result is a composite asset

        Returns:
            bool: True if the result is a composite asset
        """
        return self._is_composite_asset

    def is_collection(self) -> bool:
        """Checks if the result is a collection

        Returns:
            bool: True if the result is a collection
        """
        return self._is_collection

    def count_objects(self) -> int:
        if self._is_composite_asset:
            return len(self._paths)
        if self._is_asset:
            return 1
        if self._is_collection:
            return len(self._data.index)

    @property
    def data(self) -> pd.DataFrame | dict[str, bytes] | bytes | core.ResultCollection:
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

    def get_path(self) -> str:
        assert len(self._paths) == 1, (
            '`get_path` only works with exactly 1 path, '
            f'but there are {len(self._paths)}'
        )

        return self._paths[0]

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
    """A representation of a result of execution of a flow on Malevich Core

    Represents a single returned result, which can contain a collection,
    a list of collections, a single asset or a list of assets.

    To be precise, for the function:

    .. code-block:: python

            from malevich import flow, collection, CoreInterpreter
            from malevich.etl import process
            from malevich.utility import merge

            @flow()
            def data_process():
                data = collection('my_fancy_smth', df=pd.DataFrame({'a': [1, 2, 3]}))
                processed = process(data)
                output = merge(
                    processed,
                    data,
                    config={'how': 'inner', 'on': 'index'}
                )
                return output, processed

            task = data_process()
            task.interpret(CoreInterpreter())
            # list of CoreResult objects
            results = task()
            # single CoreResult object
            output = results[0]
            # list of CoreResultPayload objects
            print(results[0].get())

    The `output` variable will be a single CoreResult object, which contains
    a single CoreResultPayload object, which contains a single data frame
    (the result of the merge operation). The `processed` variable will be
    a single CoreResult object, which contains a single CoreResultPayload
    object, which contains a single collection (the result of the process
    operation).

    **Note**: The class is not intended to be constructed by user.
    """

    @staticmethod
    def is_asset(data: pd.DataFrame) -> bool:
        return len(data.columns) > 0 and data.columns[0] == "path"

    @staticmethod
    def extract_path_to_asset(path: str, user: str) -> str:
        return path.split(f"/mnt_obj/{user}/")[-1]

    def __init__(
        self,
        core_group_name: str,
        core_operation_id: str,
        core_run_id: str,
        conn_url: str,
        auth: core.AUTH,
    ) -> None:
        self.core_group_name = core_group_name
        self._conn_url = conn_url
        self._auth = auth
        self.core_operation_id = core_operation_id
        self.core_run_id = core_run_id

    @property
    def num_elements(self) -> int:
        """The number of elements (assets/collections) in the result"""
        return super().num_elements

    def __str__(self) -> str:
        return f'CoreResult(core_operation_id="{self.core_operation_id}")'

    def __repr__(self) -> str:
        return f'CoreResult(core_operation_id="{self.core_operation_id}")'

    @cache
    def get(self) -> list[CoreResultPayload]:
        """Retrieves results from the Core

        Returns a list of CoreResultPayload objects, each of which
        contains an output from a single operation. The output might
        be a collection, a list of collection, a single file (asset)
        or a list of files (composite asset).

        If you wish to get more specific results, use the following methods:

        - :meth:`get_df` to get a single data frame (if result is a collection)
        - :meth:`get_dfs` to get a list of data frames (if result is a collection)
        - :meth:`get_binary` to get a single binary (if result is an asset)
        - :meth:`get_binary_dir` to get a dict of binaries (if result is a composite asset)

        Returns:
            list[CoreResultPayload]: The list of results

        """  # noqa: E501
        collections = [
            x for x in core.get_collections_by_group_name(
                self.core_group_name,
                operation_id=self.core_operation_id,
                run_id=self.core_run_id,
                auth=self._auth,
                conn_url=self._conn_url
            ).data
        ]

        results = []
        for col in collections:
            if '#' in col.id:
                results.append(CoreResultPayload(
                    data=json.loads(col.docs[0]),
                    is_document=True,
                ))
                continue

            result = pd.DataFrame([json.loads(j.data) for j in col.docs])
            if CoreResult.is_asset(result):
# if asset
                # NOTE: Path now returned without /mnt_obj/<user>
                # prefix, but I left the code as it was
                # before, just in case

                obj_path = CoreResult.extract_path_to_asset(
                    result.path.iloc[0],
                    user=self._auth[0],
                )
                # ================================================
                try:
                    objects_ = core.get_collection_objects(
                        obj_path,
                        auth=self._auth,
                        conn_url=self._conn_url,
                        # NOTE: Maybe allow getting with recursive
                        recursive=True
                    )
# # if one file
                    if len(objects_.files) == 1:
                        object_ = core.get_collection_object(
                            obj_path + "/" + objects_.files[0],
                            auth=self._auth,
                            conn_url=self._conn_url,
                        )
                        results.append(CoreResultPayload(
                            data=[object_],
                            is_asset=True,
                            paths=[obj_path + "/" + objects_.files[0]]
                        ))
                    else:
# # if multiple files
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
# if failed, try without recursive
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
                            paths=[obj_path],
                        ))
# totall failure = keep it as a collection
                    except Exception as _:
                        results.append(CoreResultPayload(
                            data=result,
                            is_collection=True,
                        ))
# if collection
            else:
                results.append(CoreResultPayload(
                    data=result,
                    is_collection=True,
                ))

        return results

    @cache
    def get_df(self) -> pd.DataFrame:
        """Converts the result to a single data frame

        It only works if the result is a single collection,
        otherwise it raises an error. Check the number of
        elements using :attr:`num_elements` property and use only if
        it is equal to 1

        Returns:
            DataFrame: The result as a data frame

        Raises:
            NotImplementedError: If the result is not a single collection
        """
        if result := self.get():
            if len(result) == 1:
                if result[0].is_collection():
                    return result[0].data
                elif result[0].is_asset():
                    return NotImplementedError(
                        "Cannot return a single DataFrame from an asset. "
                        "Please use `get_binary` or `get_binary_dir` instead"
                    )
            else:
                raise NotImplementedError(
                    "Cannot return a single DataFrame from multiple results. "
                    "Please use `get_dfs` instead and check the number of results "
                    "using `num_elements` property"
                )
        else:
            warnings.warn(f"No results found for {self.core_group_name}")
            return pd.DataFrame()

    @cache
    def get_dfs(self) -> list[pd.DataFrame]:
        """Converts the result to a list of data frames

        If some of the results are assets, they are ignored
        in the output.

        Returns:
            list[:class:`DataFrame`]: The result as a list of data frames
        """
        if result := self.get():
            results_ = []
            for res in result:
                if res.is_collection():
                    results_.append(res.data)
                elif res.is_asset():
                    continue
            return results_
        else:
            return []

    @cache
    def get_binary(self) -> bytes:
        """Retrieves asset binary data, if the result is file asset

        It only works if the result is a single file returned
        as asset. Otherwise it raises an error. Check the number of
        elements using :attr:`num_elements` property and use only if
        it is equal to 1

        Returns:
            bytes: The binary data of the asset

        Raises:
            NotImplementedError: If the result is not a single asset
        """
        if result := self.get():
            if len(result) == 1:
                if result[0].is_collection():
                    raise NotImplementedError(
                        "Cannot return a single binary from a collection. "
                        "Please use `get_df` or `get_dfs` instead"
                    )
                elif result[0].is_asset():
                    return result[0].data
            else:
                raise NotImplementedError(
                    "Cannot return a single binary from multiple results. "
                    "Please use `get_binary_dir` instead and "
                    "check the number of results "
                    "using `num_elements` property"
                )
        else:
            warnings.warn(f"No results found for {self.core_group_name}")
            return b""

    @cache
    def get_binary_dir(self) -> dict[str, bytes]:
        """Retrieves files from assets

        Non-assets are ignored in the output

        Returns:
            dict[str, bytes]: Dict of file names and their binary data
        """
        if result := self.get():
            results_ = {}
            for res in result:
                if res.is_collection():
                    raise NotImplementedError(
                        "Cannot return a binary directory from a collection. "
                        "Please use `get_df` or `get_dfs` instead"
                    )
                elif res.is_asset():
                    if res.is_composite_asset():
                        results_.update(res.data)
                    else:
                        results_[res.get_path()] = res.data
            return results_
        else:
            warnings.warn(f"No results found for {self.core_group_name}")
            return {}

    @cache
    def get_document(
        self,
        model: type[DocumentModelType] | None = None
    ) -> dict | DocumentModelType:
        """Retrieves the document of the result

        Returns:
            dict: The document of the result
        """
        if result := self.get():
            # if result[index].is_document:
            #     return result[index].data if model is None else model(
            #         **result[index].data
            #     )
            # else:
            #     what = "asset" if result[index].is_asset() else "collection"
            #     raise NotImplementedError(
            #         "Cannot return a document from a non-document result. "
            #         f"Result at index {index} is {what}"
            #     )
            data_ = result[0].data.to_dict(orient='records')[0]
            return model(**data_) if model else data_

    @cache
    def get_documents(
        self,
        model: type[DocumentModelType] | None = None
    ) -> list[dict] | list[DocumentModelType]:
        if result := self.get():
            # if result[index].is_document:
            #     return result[index].data if model is None else model(
            #         **result[index].data
            #     )
            # else:
            #     what = "asset" if result[index].is_asset() else "collection"
            #     raise NotImplementedError(
            #         "Cannot return a document from a non-document result. "
            #         f"Result at index {index} is {what}"
            #     )
            data_ = result[0].data.to_dict(orient='records')
            if model:
                return [model(**d) for d in data_]
            return data_



class CoreLocalDFResult(BaseResult[pd.DataFrame]):
    """A specification of :func:`collection` as a result

    This class is used to wrap a collection object as result.
    It simply stores collection object as a DataFrame and
    returns it when :meth:`get` method is called.
    """

    def __init__(
        self,
        coll: Collection,
        auth: core.AUTH,
        conn_url: str = DEFAULT_CORE_HOST,
    ) -> None:
        self._coll = coll
        self._auth = auth
        self._conn_url = conn_url

    def num_elements(self) -> int:
        """The number of elements (assets/collections) in the result"""
        return 1

    def get(self) -> pd.DataFrame | None:
        """Simply extracts saved data frame

        Returns:
            :class:`DataFrame`: Saved data frame
        """
        if self._coll.collection_data is not None:
            return self._coll.collection_data

        # NOTE: Maybe it is better to try
        # to fetch collection from Core?

        # try:
        #     ids = core.get_collections_by_name(
        #         self._coll.magic(),
        #         auth=self._auth,
        #         conn_url=self._conn_url,
        #     ).ownIds

        #     if len(ids) == 1:
        #         return core.get_collection_to_df(
        #             ids[0],
        #             auth=self._auth,
        #             conn_url=self._conn_url,
        #         )
        #     else:
        #         return [
        #             core.get_collection_to_df(
        #                 i,
        #                 auth=self._auth,
        #                 conn_url=self._conn_url,
        #             )
        #             for i in ids

        #         ]
        # except Exception as _:
        #     return None

