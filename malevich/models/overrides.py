import os

import pandas as pd
from pydantic import BaseModel, ConfigDict


class Override(BaseModel): ...

class AssetOverride(Override):
    path: str
    file: os.PathLike | None
    folder: os.PathLike | None
    files: list[os.PathLike] | None

class CollectionOverride(Override):
    model_config = ConfigDict(
        arbitrary_types_allowed=True
    )

    data: pd.DataFrame

class DocumentOverride(Override):
    data: BaseModel

