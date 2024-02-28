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
    *,
    file: Optional[str] = None,
    df: Optional[pd.DataFrame] = None,
    scheme: SchemaMetadata = None,
    alias: Optional[str] = None,
    persistent: bool = False,
) -> gn.traced[CollectionNode]:
    """Creates a collection from a file or a dataframe

    Collection holds tabular data and is subject to be passed to processors.

    Empty collections are allowed and can be used as placeholders
    but will work only when running with Runners.

    .. warning::

        The :func:`collection <malevich._meta.collection>` function can only
        be used inside the :func:`flow <malevich._meta.flow>` function.

    Args:
        name (str, optional): Name of the collection. Defaults to None.
        file (Optional[str], optional): Path to the file. Defaults to None.
        df (Optional[pd.DataFrame], optional): Dataframe. Defaults to None.
        scheme (Optional[SchemaMetadata], optional): Schema of the collection.
        alias (Optional[str], optional):
            An optional short name for the collection.
            If not set, will be infered from the name.
        persistent (bool, optional):
            If the flag is set, the collection is only updated
            when the collection is not present on the platform.
            If persistent collection with the same name is already present,
            the data is not updated. Defaults to False.

    Raises:
        AssertionError: If both file and data are provided

    Returns:
        Collection: a traced object that is subject to be passed to processors
    """
    # Commented in favor of empty collections
    # for runners (Jan 10, 2024, v0.1.2)

    # assert any(
    #     [file is not None, df is not None]
    # ), "Either file or data must be provided"
    # ______________________________________

    if file:
        assert df is None, "Cannot provide both file and data"
        df = pd.read_csv(file)

    name = name or 'auto-collection-' + uuid4().hex[:8]

    # Retrieve JSON Schema
    # from pandas DataFrame

    if df is None:
        df = pd.DataFrame()

    pd_scheme = scheme or json.dumps(pd_to_json_schema(df))
    node = CollectionNode(
        collection=Collection(
            collection_id=name,
            collection_data=df,
            persistent=persistent,
        ),
        scheme=pd_scheme,
    )

    if alias:
        node.alias = alias
    else:
        node.alias = name

    return gn.traced(node)
