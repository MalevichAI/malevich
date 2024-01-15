from functools import cache

import pandas as pd
from malevich_space.ops import SpaceOps

from ..base import BaseResult


class SpaceCollectionResult(BaseResult[list[pd.DataFrame]]):
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

    @cache
    def get(self) -> pd.DataFrame:
        return [
            pd.DataFrame(x.raw_json)
            for x in self._ops.get_results(
                run_id=self.run_id,
                in_flow_id=self.in_flow_id,
            )
        ]
