from typing import Optional
from uuid import uuid4

import pandas as pd

from .._autoflow import tracer as gn  # engine
from ..models.collection import Collection
from ..models.nodes import CollectionNode


def collection(
    name: Optional[str] = None,
    file: Optional[str] = None,
    data: Optional[pd.DataFrame] = None
) -> gn.traced[CollectionNode]:
    """Creates a collection from a file or a dataframe

    Args:
        name (Optional[str], optional): Name of the collection. Defaults to None.
        file (Optional[str], optional): Path to the file. Defaults to None.
        data (Optional[pd.DataFrame], optional): Dataframe. Defaults to None.

    Raises:
        AssertionError: If both file and data are provided

    Returns:
        Collection: an object that is subject to be passed to processors
    """
    assert any([file is not None, data is not None]), \
        "Either file or data must be provided"

    if file:
        assert data is None, "Cannot provide both file and data"
        data = pd.read_csv(file)

    name = name or uuid4().hex
    return gn.traced(
        CollectionNode(
            collection=Collection(collection_id=name, collection_data=data)
        )
    )
