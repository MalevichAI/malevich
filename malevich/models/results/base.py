from abc import ABC, abstractmethod
from typing import Generic, Type, TypeVar, get_args

RealResultType = TypeVar("RealResultType")

class BaseResult(ABC, Generic[RealResultType]):
    """Result obtained running a flow.

    Encapsulates the result object obtained after running a flow.
    The object can be a DataFrame, a list of DataFrames, binary data, etc.
    """

    def result_type(self) -> Type:
        return get_args(self.__orig_class__)[0]

    @abstractmethod
    def get(self) -> RealResultType:
        """Fetches a real result object (download, etc.)

        Returns:
            RealResultType: The actual result
        """
        pass
