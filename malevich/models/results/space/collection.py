from functools import cache

import pandas as pd
from malevich_space.ops import SpaceOps
from pandas.core.api import DataFrame as DataFrame

from ..base import BaseResult


class SpaceCollectionResult(BaseResult[list[pd.DataFrame]]):
    """Result obtained running a flow with Space.

    Represents a set of collections obtained after running
    a flow with Space.
    """
    def __init__(
        self,
        run_id: str,
        in_flow_id: str,
        space_ops: SpaceOps
    ) -> None:
        super().__init__()
        self.run_id = run_id
        self.in_flow_id = in_flow_id
        self._ops = space_ops

    def num_elements(self) -> int:
        return len(self.get())

    @cache
    def get(self) -> list[pd.DataFrame]:
        """Fetches a list of DataFrames with the actual result

        Returns:
            list[:class:`DataFrame`]: The actual result
        """
        return [
            pd.DataFrame(x.raw_json)
            for x in self._ops.get_results(
                run_id=self.run_id,
                in_flow_id=self.in_flow_id,
            )
        ]

    @cache
    def get_dfs(self) -> list[DataFrame]:
        """Returns a list of DataFrames from the result.

        Simply calls :meth:`get()`

        Returns:
            list[:class:`DataFrame`]: The list of DataFrames
        """
        return self.get()

    @cache
    def get_df(self) -> DataFrame:
        """Returns a DataFrame from the result if it is a single DataFrame.

        Simply calls :meth:`get()` and returns the first element
        if the list has only one element, otherwise raises an error.

        Returns:
            :class:`DataFrame`: The result DataFrame
        """
        if len(results := self.get()) == 1:
            return results[0]
