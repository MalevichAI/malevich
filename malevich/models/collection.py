import uuid

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field


class Collection(BaseModel):
    collection_id: str
    collection_data: pd.DataFrame = Field(..., repr=False)
    model_config = ConfigDict(
        arbitrary_types_allowed=True
    )

    @staticmethod
    def from_file(file: str, id: None = uuid.uuid4()) -> None:
        return Collection(
            collection_id=id,
            collection_data=pd.read_csv(file)
        )
