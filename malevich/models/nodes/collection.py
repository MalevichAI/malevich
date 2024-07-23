import base64
import tempfile
from typing import Any

import pandas as pd
from malevich_space.schema.schema import SchemaMetadata

from malevich.models import Collection

from .base import BaseNode


class CollectionNode(BaseNode):
    collection: Collection
    scheme: str | SchemaMetadata | None = None

    def get_senstivite_fields(self) -> dict[str, Any]:
        # Save pd.DataFrame as binary data and encode
        # it with base64 to save as string
        data = self.collection.collection_data
        with tempfile.TemporaryFile() as f:
            data.to_pickle(f)
            f.seek(0)
            encoded = base64.b64encode(f.read()).decode()
        if data is None:
            return {}
        return {"collection_data": encoded}

    def set_sensitive_fields(self, values: dict[str, str]) -> None:
        collection_data = values.get("collection_data")
        if collection_data is None:
            return
        decoded = base64.b64decode(collection_data)
        with tempfile.TemporaryFile() as f:
            f.write(decoded)
            f.seek(0)
            data = pd.read_pickle(f)
        self.collection.collection_data = data

    def __hash__(self) -> int:
        return super().__hash__()
