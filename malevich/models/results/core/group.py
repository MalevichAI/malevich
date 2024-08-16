from typing import Any
from .result import CoreResult, CoreResultPayload


class FirstValidResultProxy(CoreResult):
    def __init__(self, results: list[CoreResult]):
        self._results = results
        self._valid: int = None


    def __getattribute__(self, name: str) -> Any:
        if self._valid is not None:
            import malevich_coretools


    @cache
    def get(self) -> list[CoreResultPayload]:
        if self._valid is not None:
            return
           

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

