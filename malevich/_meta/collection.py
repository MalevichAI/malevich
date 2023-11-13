import json
from typing import Optional
from uuid import uuid4

import pandas as pd
from malevich_space.schema.schema import SchemaMetadata

from .._autoflow import tracer as gn  # engine
from .._utility.schema import pd_to_json_schema
from ..models.collection import Collection
from ..models.nodes import CollectionNode


def collection(
    name: str,
    file: Optional[str] = None,
    df: Optional[pd.DataFrame] = None,
    scheme: SchemaMetadata = None,
    alias: Optional[str] = None,
) -> gn.traced[CollectionNode]:
    """Creates a collection from a file or a dataframe

    The name might be required when working with some of interpreters

    Args:
        name (str, optional): Name of the collection. Defaults to None.
        file (Optional[str], optional): Path to the file. Defaults to None.
        df (Optional[pd.DataFrame], optional): Dataframe. Defaults to None.
        scheme (Optional[SchemaMetadata], optional): Schema of the collection.

    Raises:
        AssertionError: If both file and data are provided

    Returns:
        Collection: an object that is subject to be passed to processors
    """
    assert any(
        [file is not None, df is not None]
    ), "Either file or data must be provided"

    if file:
        assert df is None, "Cannot provide both file and data"
        df = pd.read_csv(file)

    name = name or 'auto-collection-' + uuid4().hex[:8]

    # Retrieve JSON Schema
    # from pandas DataFrame

    pd_scheme = scheme or json.dumps(pd_to_json_schema(df))
    node = CollectionNode(
        collection=Collection(
            collection_id=name,
            collection_data=df,
        ),
        scheme=pd_scheme,
    )

    if alias:
        node.alias = alias

    return gn.traced(node)
