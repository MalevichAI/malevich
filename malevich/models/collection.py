import hashlib
import io
import uuid
from typing import Optional

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from malevich.models.python_string import PythonString


class Collection(BaseModel):
    collection_id: PythonString
    core_id: Optional[str] = None
    collection_data: Optional[pd.DataFrame] = Field(
        pd.DataFrame([]), repr=False
    )

    model_config = ConfigDict(
        arbitrary_types_allowed=True
    )

    persistent: bool = False

    @staticmethod
    def from_file(file: str, id: None = uuid.uuid4()) -> None:
        return Collection(
            collection_id=id,
            collection_data=pd.read_csv(file)
        )

    def magic(self, with_id=None, with_data=None) -> str:
        if with_id is None:
            with_id = True
        if with_data is None:
            with_data = not self.persistent

        buffer = io.BytesIO()
        self.collection_data.to_pickle(buffer)
        buffer.seek(0)
        data_ = buffer.read()

        a = hashlib.sha256(data_).hexdigest()
        b = hashlib.sha256(self.collection_id.encode()).hexdigest()

        if not with_id:
            b = ""
        if not with_data:
            a = ""

        if with_id and with_data:
            return a[:32] + b[:32]
        elif with_id:
            return b
        elif with_data:
            return a

    def verify(self, hash: str, **kwargs) -> bool:
        return self.magic(**kwargs) == hash
