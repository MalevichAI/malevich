from abc import ABC, abstractmethod
from typing import Generic, Type, TypeVar, get_args, overload

import pandas as pd
from pydantic import BaseModel

RealResultType = TypeVar("RealResultType")
DocumentType = TypeVar("DocumentType", bound=BaseModel)

class BaseResult(ABC, Generic[RealResultType]):
    """Result obtained running a flow.

    Encapsulates the result object obtained after running a flow.
    The object can be a DataFrame, a list of DataFrames, binary data, etc.
    """

    def result_type(self) -> Type:
        return get_args(self.__orig_class__)[0]

    @property
    @abstractmethod
    def num_elements(self) -> int:
        """The number of elements (assets/collections) in the result"""
        pass

    @abstractmethod
    def get(self) -> RealResultType:
        """Fetches a real result object (download, etc.)

        Returns:
            RealResultType: The actual result
        """
        pass

    def get_df(self) -> pd.DataFrame:
        """Returns a DataFrame from the result if possible

        Returns:
            pd.DataFrame: The DataFrame
        """
        raise NotImplementedError(
            f"Result {self.__class__.__name__} does not support "
            "conversion to DataFrame"
        )

    def get_dfs(self) -> list[pd.DataFrame]:
        """Returns a list of DataFrames from the result if possible

        Returns:
            list[pd.DataFrame]: The list of DataFrames
        """
        raise NotImplementedError(
            f"Result {self.__class__.__name__} does not support "
            "conversion to list of DataFrames"
        )

    def get_binary(self) -> bytes:
        """Returns binary data from the result if possible

        Returns:
            bytes: The binary data
        """
        raise NotImplementedError(
            f"Result {self.__class__.__name__} does not support "
            "conversion to binary data"
        )

    def get_binary_dir(self) -> dict[str, bytes]:
        """Returns a dict of binary data from the result if possible

        Returns:
            dict[str, bytes]: The dict of binary data
        """
        raise NotImplementedError(
            f"Result {self.__class__.__name__} does not support "
            "conversion to dict of binary data"
        )


    @overload
    def get_document(self, index: int = 0) -> dict:
        """Returns dumped document from the result if possible

        Returns:
            dict: The dumped document
        """
        pass

    @overload
    def get_document(self, index:int = 0, *, model: Type[DocumentType]) -> DocumentType:
        """Returns dumped document from the result if possible

        Returns:
            dict: The dumped document
        """
        pass

    def get_document(
        self,
        index: int = 0,
        *,
        model: Type[DocumentType] | None = None
    ) -> DocumentType:
        """Returns a document from the result if possible

        Returns:
            DocumentType: The document
        """
        pass
